#!/usr/bin/env python

# MBUtil: a tool for MBTiles files
# Supports importing, exporting, and more
#
# (c) Development Seed 2012
# Licensed under BSD

# for additional reference on schema see:
# https://github.com/mapbox/node-mbtiles/blob/master/lib/schema.sql

import json
import logging
import os
import re
import sqlite3
import sys
import time
import zlib

logger = logging.getLogger(__name__)

def flip_y(zoom, y):
    return (2**zoom-1) - y

def mbtiles_setup(cur):
    cur.execute("""
        create table tiles (
            zoom_level integer,
            tile_column integer,
            tile_row integer,
            tile_data blob);
            """)
    cur.execute("""create table metadata
        (name text, value text);""")
    cur.execute("""CREATE TABLE grids (zoom_level integer, tile_column integer,
    tile_row integer, grid blob);""")
    cur.execute("""CREATE TABLE grid_data (zoom_level integer, tile_column
    integer, tile_row integer, key_name text, key_json text);""")
    cur.execute("""create unique index name on metadata (name);""")
    cur.execute("""create unique index tile_index on tiles
        (zoom_level, tile_column, tile_row);""")

def mbtiles_connect(mbtiles_file, silent):
    try:
        con = sqlite3.connect(mbtiles_file)
        return con
    except Exception as e:
        if not silent:
            logger.error("Could not connect to database")
            logger.exception(e)
        sys.exit(1)

def optimize_connection(cur):
    cur.execute("""PRAGMA synchronous=0""")
    cur.execute("""PRAGMA locking_mode=EXCLUSIVE""")
    cur.execute("""PRAGMA journal_mode=DELETE""")

def compression_prepare(cur, silent):
    if not silent: 
        logger.debug('Prepare database compression.')
    cur.execute("""
      CREATE TABLE if not exists images (
        tile_data blob,
        tile_id integer);
    """)
    cur.execute("""
      CREATE TABLE if not exists map (
        zoom_level integer,
        tile_column integer,
        tile_row integer,
        tile_id integer);
    """)


def optimize_database(con, silent):
    cur = con.cursor()
    if not silent:
        logger.debug('analyzing db')
    cur.execute("""ANALYZE;""")
    if not silent:
        logger.debug('cleaning db')

    # Seadista isolation_level ühenduse objektil, mitte kursoril
    con.isolation_level = None  # Workaround for sqlite3 vacuum operatsioon
    cur.execute("""VACUUM;""")
    con.isolation_level = ''  # Taasta vaikimisi isolation_level

    cur.execute("""ANALYZE;""")


def compression_do(cur, con, chunk, silent):
    if not silent:
        logger.debug('Making database compression.')
    overlapping = 0
    unique = 0
    total = 0
    cur.execute("select count(zoom_level) from tiles")
    res = cur.fetchone()
    total_tiles = res[0]
    last_id = 0
    if not silent:
        logging.debug("%d total tiles to fetch" % total_tiles)
    for i in range(total_tiles // chunk + 1):
        if not silent:
            logging.debug("%d / %d rounds done" % (i, (total_tiles / chunk)))
        ids = []
        files = []
        start = time.time()
        cur.execute("""select zoom_level, tile_column, tile_row, tile_data
            from tiles where rowid > ? and rowid <= ?""", ((i * chunk), ((i + 1) * chunk)))
        if not silent:
            logger.debug("select: %s" % (time.time() - start))
        rows = cur.fetchall()
        for r in rows:
            total = total + 1
            if r[3] in files:
                overlapping = overlapping + 1
                start = time.time()
                query = """insert into map
                    (zoom_level, tile_column, tile_row, tile_id)
                    values (?, ?, ?, ?)"""
                if not silent:
                    logger.debug("insert: %s" % (time.time() - start))
                cur.execute(query, (r[0], r[1], r[2], ids[files.index(r[3])]))
            else:
                unique = unique + 1
                last_id += 1

                ids.append(last_id)
                files.append(r[3])

                start = time.time()
                query = """insert into images
                    (tile_id, tile_data)
                    values (?, ?)"""
                cur.execute(query, (str(last_id), sqlite3.Binary(r[3])))
                if not silent:
                    logger.debug("insert into images: %s" % (time.time() - start))
                start = time.time()
                query = """insert into map
                    (zoom_level, tile_column, tile_row, tile_id)
                    values (?, ?, ?, ?)"""
                cur.execute(query, (r[0], r[1], r[2], last_id))
                if not silent:
                    logger.debug("insert into map: %s" % (time.time() - start))
        con.commit()

def compression_finalize(cur, con, silent):
    if not silent:
        logger.debug('Finalizing database compression.')
    cur.execute("""drop table tiles;""")
    cur.execute("""create view tiles as
        select map.zoom_level as zoom_level,
        map.tile_column as tile_column,
        map.tile_row as tile_row,
        images.tile_data as tile_data FROM
        map JOIN images on images.tile_id = map.tile_id;""")
    cur.execute("""
          CREATE UNIQUE INDEX map_index on map
            (zoom_level, tile_column, tile_row);""")
    cur.execute("""
          CREATE UNIQUE INDEX images_id on images
            (tile_id);""")

    # Workaround for python>=3.6.0,python<3.6.2
    # https://bugs.python.org/issue28518
    con.isolation_level = None
    cur.execute("""vacuum;""")
    con.isolation_level = ''  # reset default value of isolation_level

    cur.execute("""analyze;""")

def get_dirs(path):
    return [name for name in os.listdir(path)
        if os.path.isdir(os.path.join(path, name))]

def disk_to_mbtiles(directory_path, mbtiles_file, **kwargs):
    silent = kwargs.get('silent')

    if not silent:
        logger.info("Importing disk to MBTiles")
        logger.debug("%s --> %s" % (directory_path, mbtiles_file))

    # Ühendus MBTiles failiga
    con = mbtiles_connect(mbtiles_file, silent)
    cur = con.cursor()
    optimize_connection(cur)
    mbtiles_setup(cur)

    # Metaandmete lugemine ja vaikimisi pildiformaat
    image_format = kwargs.get('format', 'pbf')  # Vaikimisi pbf
    scheme = kwargs.get('scheme', 'xyz')  # Vaikimisi xyz

    try:
        metadata_path = os.path.join(directory_path, 'metadata.json')
        with open(metadata_path, 'r') as metadata_file:
            metadata = json.load(metadata_file)
        for name, value in metadata.items():
            cur.execute('INSERT INTO metadata (name, value) VALUES (?, ?)', (name, value))
        if not silent:
            logger.info('Metadata from metadata.json restored')

        # Kui metadata määrab pildiformaadi, kasuta seda
        image_format = metadata.get('format', image_format)
    except FileNotFoundError:
        if not silent:
            logger.warning('metadata.json not found. Using default values.')

    count = 0
    start_time = time.time()

    # Zoomi kaustade järjestamine väiksemast suurimani
    zoom_levels = sorted(get_dirs(directory_path), key=lambda z: int(re.sub(r"[^\d]", "", z)))

    for zoom_dir in zoom_levels:
        z = int(re.sub(r"[^\d]", "", zoom_dir))  # Võta zoomi taseme number
        zoom_path = os.path.join(directory_path, zoom_dir)

        for row_dir in sorted(get_dirs(zoom_path)):
            x = int(re.sub(r"[^\d]", "", row_dir))  # Tile'i veeru number
            row_path = os.path.join(zoom_path, row_dir)

            for current_file in sorted(os.listdir(row_path)):
                # Süsteemifailide ignoreerimine
                if current_file.startswith("."):
                    continue

                # Failinime ja laiendi jagamine
                file_name, ext = os.path.splitext(current_file)
                ext = ext.lstrip('.')  # Eemalda juhtiv punkt

                if ext != image_format:
                    if not silent:
                        logger.warning(f"Skipping {current_file} (not {image_format})")
                    continue

                # Tile'i rea number ja faili sisu
                y = int(file_name)
                tile_path = os.path.join(row_path, current_file)
                with open(tile_path, 'rb') as tile_file:
                    tile_data = tile_file.read()

                # Y-telje peegeldamine TMS-skeemi puhul
                if scheme == 'tms':
                    y = flip_y(z, y)

                # Tile'i andmete sisestamine andmebaasi
                cur.execute("""
                    INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data)
                    VALUES (?, ?, ?, ?)
                """, (z, x, y, sqlite3.Binary(tile_data)))

                count += 1
                if count % 100 == 0 and not silent:
                    elapsed_time = time.time() - start_time
                    logger.info(f"{count} tiles inserted ({count / elapsed_time:.2f} tiles/sec)")

    if not silent:
        logger.info(f"Inserted {count} tiles in total.")

    # Kui kompressioon on lubatud, tee vajalikud sammud
    if kwargs.get('compression', False):
        compression_prepare(cur, silent)
        compression_do(cur, con, 256, silent)
        compression_finalize(cur, con, silent)

    # Optimeeri andmebaas ja sulge ühendus
    optimize_database(con, silent)  # Edasta `con` (ühendus) objekt, mitte `cur`
    con.commit()
    con.close()

    if not silent:
        logger.info("MBTiles file created successfully.")


def mbtiles_metadata_to_disk(mbtiles_file, **kwargs):
    silent = kwargs.get('silent')
    if not silent:
        logger.debug("Exporting MBTiles metatdata from %s" % (mbtiles_file))
    con = mbtiles_connect(mbtiles_file, silent)
    metadata = dict(con.execute('select name, value from metadata;').fetchall())
    if not silent:
        logger.debug(json.dumps(metadata, indent=2))

def mbtiles_to_disk(mbtiles_file, directory_path, **kwargs):
    silent = kwargs.get('silent')
    if not silent:
        logger.debug("Exporting MBTiles to disk")
        logger.debug("%s --> %s" % (mbtiles_file, directory_path))
    con = mbtiles_connect(mbtiles_file, silent)
    os.mkdir("%s" % directory_path)

    # Metadata lugemine ja failivormingu valimine
    metadata = dict(con.execute('select name, value from metadata;').fetchall())
    json.dump(metadata, open(os.path.join(directory_path, 'metadata.json'), 'w'), indent=4)
    image_format = metadata.get('format', 'pbf')  # Kasuta metadata formaati või vaikimisi .pbf

    if not silent:
        logger.info("Tile format detected: %s" % image_format)


    count = con.execute('select count(zoom_level) from tiles;').fetchone()[0]
    done = 0
    base_path = directory_path
    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    # if interactivity
    formatter = metadata.get('formatter')
    if formatter:
        layer_json = os.path.join(base_path, 'layer.json')
        formatter_json = {"formatter":formatter}
        open(layer_json, 'w').write(json.dumps(formatter_json))

    tiles = con.execute(
        'select zoom_level, tile_column, tile_row, tile_data from tiles ORDER BY zoom_level ASC;')
    t = tiles.fetchone()
    while t:
        z = t[0]
        x = t[1]
        y = t[2]
        if kwargs.get('scheme') == 'xyz':
            y = flip_y(z,y)
            if not silent:
                logger.debug('flipping')
            tile_dir = os.path.join(base_path, str(z), str(x))
        elif kwargs.get('scheme') == 'wms':
            tile_dir = os.path.join(base_path,
                "%02d" % (z),
                "%03d" % (int(x) / 1000000),
                "%03d" % ((int(x) / 1000) % 1000),
                "%03d" % (int(x) % 1000),
                "%03d" % (int(y) / 1000000),
                "%03d" % ((int(y) / 1000) % 1000))
        else:
            tile_dir = os.path.join(base_path, str(z), str(x))
        if not os.path.isdir(tile_dir):
            os.makedirs(tile_dir)
        if kwargs.get('scheme') == 'wms':
            tile = os.path.join(tile_dir,'%03d.%s' % (int(y) % 1000, kwargs.get('format', 'png')))
        else:
            tile = os.path.join(tile_dir, f'{y}.{image_format}')
        f = open(tile, 'wb')
        f.write(t[3])
        f.close()
        done = done + 1
        if not silent:
            logger.info('%s / %s tiles exported' % (done, count))
        t = tiles.fetchone()

    # grids
    callback = kwargs.get('callback')
    done = 0
    try:
        count = con.execute('select count(zoom_level) from grids;').fetchone()[0]
        grids = con.execute('select zoom_level, tile_column, tile_row, grid from grids;')
        g = grids.fetchone()
    except sqlite3.OperationalError:
        g = None # no grids table
    while g:
        zoom_level = g[0] # z
        tile_column = g[1] # x
        y = g[2] # y
        grid_data_cursor = con.execute('''select key_name, key_json FROM
            grid_data WHERE
            zoom_level = %(zoom_level)d and
            tile_column = %(tile_column)d and
            tile_row = %(y)d;''' % locals() )
        if kwargs.get('scheme') == 'xyz':
            y = flip_y(zoom_level,y)
        grid_dir = os.path.join(base_path, str(zoom_level), str(tile_column))
        if not os.path.isdir(grid_dir):
            os.makedirs(grid_dir)
        grid = os.path.join(grid_dir,'%s.grid.json' % (y))
        f = open(grid, 'w')
        grid_json = json.loads(zlib.decompress(g[3]).decode('utf-8'))
        # join up with the grid 'data' which is in pieces when stored in mbtiles file
        grid_data = grid_data_cursor.fetchone()
        data = {}
        while grid_data:
            data[grid_data[0]] = json.loads(grid_data[1])
            grid_data = grid_data_cursor.fetchone()
        grid_json['data'] = data
        if callback in (None, "", "false", "null"):
            f.write(json.dumps(grid_json))
        else:
            f.write('%s(%s);' % (callback, json.dumps(grid_json)))
        f.close()
        done = done + 1
        if not silent:
            logger.info('%s / %s grids exported' % (done, count))
        g = grids.fetchone()
