#!/usr/bin/env bash
# Tags: no-fasttest
#       ^ no Parquet support in fasttest
# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query="
    DROP TABLE IF EXISTS geom1;
    CREATE TABLE IF NOT EXISTS geom1 (point Point) ENGINE = Memory();
    INSERT INTO geom1 VALUES((10, 20));
    INSERT INTO geom1 VALUES((30, 40));
    SELECT * FROM geom1 ORDER BY point FORMAT Parquet;" > "${CLICKHOUSE_TMP}/parquet_geom1.parquet"
$CLICKHOUSE_LOCAL --query="SELECT * FROM file('${CLICKHOUSE_TMP}/parquet_geom1.parquet', Parquet);"

$CLICKHOUSE_LOCAL --query="
    DROP TABLE IF EXISTS geom2;
    CREATE TABLE IF NOT EXISTS geom2 (point LineString) ENGINE = Memory();
    INSERT INTO geom2 VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
    SELECT * FROM geom2 ORDER BY point FORMAT Parquet;" > "${CLICKHOUSE_TMP}/parquet_geom2.parquet"
$CLICKHOUSE_LOCAL --query="SELECT * FROM file('${CLICKHOUSE_TMP}/parquet_geom2.parquet', Parquet);"

$CLICKHOUSE_LOCAL --query="
    DROP TABLE IF EXISTS geom3;
    CREATE TABLE IF NOT EXISTS geom3 (point Polygon) ENGINE = Memory();
    INSERT INTO geom3 VALUES([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]]);
    SELECT * FROM geom3 ORDER BY point FORMAT Parquet;" > "${CLICKHOUSE_TMP}/parquet_geom3.parquet"
$CLICKHOUSE_LOCAL --query="SELECT * FROM file('${CLICKHOUSE_TMP}/parquet_geom3.parquet', Parquet);"

$CLICKHOUSE_LOCAL --query="
    DROP TABLE IF EXISTS geom4;
    CREATE TABLE IF NOT EXISTS geom4 (point MultiLineString) ENGINE = Memory();
    INSERT INTO geom4 VALUES([[(0, 0), (10, 0), (10, 10), (0, 10)], [(1, 1), (2, 2), (3, 3)]]);
    SELECT * FROM geom4 ORDER BY point FORMAT Parquet;" > "${CLICKHOUSE_TMP}/parquet_geom4.parquet"
$CLICKHOUSE_LOCAL --query="SELECT * FROM file('${CLICKHOUSE_TMP}/parquet_geom4.parquet', Parquet);"

$CLICKHOUSE_LOCAL --query="
    DROP TABLE IF EXISTS geom5;
    CREATE TABLE IF NOT EXISTS geom5 (point MultiPolygon) ENGINE = Memory();
    INSERT INTO geom5 VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]);
    SELECT * FROM geom5 ORDER BY point FORMAT Parquet;" > "${CLICKHOUSE_TMP}/parquet_geom5.parquet"
$CLICKHOUSE_LOCAL --query="SELECT * FROM file('${CLICKHOUSE_TMP}/parquet_geom5.parquet', Parquet);"

$CLICKHOUSE_LOCAL --query="
    DROP TABLE IF EXISTS geom6;
    CREATE TABLE IF NOT EXISTS geom6 (point Point) ENGINE = Memory();
    INSERT INTO geom6 VALUES((10, 20));
    INSERT INTO geom6 VALUES((30, 40));
    SELECT * FROM geom6 ORDER BY point FORMAT Parquet SETTINGS output_format_parquet_geometadata=false;" > "${CLICKHOUSE_TMP}/parquet_geom6.parquet"
$CLICKHOUSE_LOCAL --query="SELECT * FROM file('${CLICKHOUSE_TMP}/parquet_geom6.parquet', Parquet);"
