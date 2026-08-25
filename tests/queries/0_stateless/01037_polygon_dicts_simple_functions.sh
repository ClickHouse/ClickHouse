#!/usr/bin/env bash
# Tags: no-debug

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TMP_DIR=${CLICKHOUSE_TMP}/tmp
mkdir -p $TMP_DIR

$CLICKHOUSE_CLIENT --query="
DROP TABLE IF EXISTS polygons_array;

CREATE TABLE polygons_array (key Array(Array(Array(Array(Float64)))), name String, value UInt64) ENGINE = Memory;
INSERT INTO polygons_array VALUES ([[[[1, 3], [1, 1], [3, 1], [3, -1], [1, -1], [1, -3], [-1, -3], [-1, -1], [-3, -1], [-3, 1], [-1, 1], [-1, 3]]], [[[5, 5], [5, 1], [7, 1], [7, 7], [1, 7], [1, 5]]]], 'Click', 42);
INSERT INTO polygons_array VALUES ([[[[5, 5], [5, -5], [-5, -5], [-5, 5]], [[1, 3], [1, 1], [3, 1], [3, -1], [1, -1], [1, -3], [-1, -3], [-1, -1], [-3, -1], [-3, 1], [-1, 1], [-1, 3]]]], 'House', 314159);
INSERT INTO polygons_array VALUES ([[[[3, 1], [0, 1], [0, -1], [3, -1]]]], 'Click East', 421);
INSERT INTO polygons_array VALUES ([[[[-1, 1], [1, 1], [1, 3], [-1, 3]]]], 'Click North', 422);
INSERT INTO polygons_array VALUES ([[[[-3, 1], [-3, -1], [0, -1], [0, 1]]]], 'Click South', 423);
INSERT INTO polygons_array VALUES ([[[[-1, -1], [1, -1], [1, -3], [-1, -3]]]], 'Click West', 424);

DROP TABLE IF EXISTS polygons_tuple;

CREATE TABLE polygons_tuple (key Array(Array(Array(Tuple(Float64, Float64)))), name String, value UInt64) ENGINE = Memory;
INSERT INTO polygons_tuple VALUES ([[[(1, 3), (1, 1), (3, 1), (3, -1), (1, -1), (1, -3), (-1, -3), (-1, -1), (-3, -1), (-3, 1), (-1, 1), (-1, 3)]], [[(5, 5), (5, 1), (7, 1), (7, 7), (1, 7), (1, 5)]]], 'Click', 42);
INSERT INTO polygons_tuple VALUES ([[[(5, 5), (5, -5), (-5, -5), (-5, 5)], [(1, 3), (1, 1), (3, 1), (3, -1), (1, -1), (1, -3), (-1, -3), (-1, -1), (-3, -1), (-3, 1), (-1, 1), (-1, 3)]]], 'House', 314159);
INSERT INTO polygons_tuple VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Click East', 421);
INSERT INTO polygons_tuple VALUES ([[[(-1, 1), (1, 1), (1, 3), (-1, 3)]]], 'Click North', 422);
INSERT INTO polygons_tuple VALUES ([[[(-3, 1), (-3, -1), (0, -1), (0, 1)]]], 'Click South', 423);
INSERT INTO polygons_tuple VALUES ([[[(-1, -1), (1, -1), (1, -3), (-1, -3)]]], 'Click West', 424);

DROP TABLE IF EXISTS points;

CREATE TABLE points (x Float64, y Float64, def_i UInt64, def_s String) ENGINE = Memory;
INSERT INTO points VALUES (0.1, 0.0, 112, 'aax');
INSERT INTO points VALUES (-0.1, 0.0, 113, 'aay');
INSERT INTO points VALUES (0.0, 1.1, 114, 'aaz');
INSERT INTO points VALUES (0.0, -1.1, 115, 'aat');
INSERT INTO points VALUES (3.0, 3.0, 22, 'bb');
INSERT INTO points VALUES (5.0, 6.0, 33, 'cc');
INSERT INTO points VALUES (-100.0, -42.0, 44, 'dd');
INSERT INTO points VALUES (7.01, 7.01, 55, 'ee');
INSERT INTO points VALUES (0.99, 2.99, 66, 'ee');
INSERT INTO points VALUES (1.0, 0.0, 771, 'ffa');
INSERT INTO points VALUES (-1.0, 0.0, 772, 'ffb');
INSERT INTO points VALUES (0.0, 2.0, 773, 'ffc');
INSERT INTO points VALUES (0.0, -2.0, 774, 'ffd');
"

declare -a SearchTypes=("POLYGON" "POLYGON_SIMPLE" "POLYGON_INDEX_EACH" "POLYGON_INDEX_CELL")

for type in "${SearchTypes[@]}";
do
    outputFile="${TMP_DIR}/results${type}.out"

    $CLICKHOUSE_CLIENT --query="
    DROP DICTIONARY IF EXISTS dict_array;
    CREATE DICTIONARY dict_array
    (
    key Array(Array(Array(Array(Float64)))),
    name String DEFAULT 'qqq',
    value UInt64 DEFAULT 101
    )
    PRIMARY KEY key
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'polygons_array' PASSWORD '' DB currentDatabase()))
    LIFETIME(0)
    LAYOUT($type());

    DROP DICTIONARY IF EXISTS dict_tuple;

    CREATE DICTIONARY dict_tuple
    (
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String DEFAULT 'qqq',
    value UInt64 DEFAULT 101
    )
    PRIMARY KEY key
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'polygons_tuple' PASSWORD '' DB currentDatabase()))
    LIFETIME(0)
    LAYOUT($type());

    select 'dictGet', 'dict_array' as dict_name, tuple(x, y) as key,
        dictGet(dict_name, 'name', key),
        dictGet(dict_name, 'value', key) from points order by x, y;
    select 'dictGetOrDefault', 'dict_array' as dict_name, tuple(x, y) as key,
        dictGetOrDefault(dict_name, 'name', key, 'www'),
        dictGetOrDefault(dict_name, 'value', key, toUInt64(1234)) from points order by x, y;
    select 'dictGetOrDefault', 'dict_array' as dict_name, tuple(x, y) as key,
        dictGetOrDefault(dict_name, 'name', key, def_s),
        dictGetOrDefault(dict_name, 'value', key, def_i) from points order by x, y;
    select 'dictGet', 'dict_tuple' as dict_name, tuple(x, y) as key,
        dictGet(dict_name, 'name', key),
        dictGet(dict_name, 'value', key) from points order by x, y;
    select 'dictGetOrDefault', 'dict_tuple' as dict_name, tuple(x, y) as key,
        dictGetOrDefault(dict_name, 'name', key, 'www'),
        dictGetOrDefault(dict_name, 'value', key, toUInt64(1234)) from points order by x, y;
    select 'dictGetOrDefault', 'dict_tuple' as dict_name, tuple(x, y) as key,
        dictGetOrDefault(dict_name, 'name', key, def_s),
        dictGetOrDefault(dict_name, 'value', key, def_i) from points order by x, y;
    select 'dictHas', 'dict_array' as dict_name, tuple(x, y) as key,
        dictHas(dict_name, key) from points order by x, y;
    select 'dictHas', 'dict_tuple' as dict_name, tuple(x, y) as key,
        dictHas(dict_name, key) from points order by x, y;
    "  > "$outputFile"

    diff -q "${CURDIR}/01037_polygon_dicts_simple_functions.ans" "$outputFile"
done

$CLICKHOUSE_CLIENT --query="
DROP DICTIONARY dict_array;
DROP DICTIONARY dict_tuple;
DROP TABLE polygons_array;
DROP TABLE polygons_tuple;
DROP TABLE points;
"
