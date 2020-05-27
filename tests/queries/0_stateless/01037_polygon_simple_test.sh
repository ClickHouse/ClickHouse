#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

TMP_DIR="/tmp"

$CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS test_01037;"

$CLICKHOUSE_CLIENT --query="CREATE DATABASE test_01037 Engine = Ordinary;"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_01037.polygons_array;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE test_01037.polygons_array (key Array(Array(Array(Array(Float64)))), name String, value UInt64) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_array VALUES ([[[[1, 3], [1, 1], [3, 1], [3, -1], [1, -1], [1, -3], [-1, -3], [-1, -1], [-3, -1], [-3, 1], [-1, 1], [-1, 3]]], [[[5, 5], [5, 1], [7, 1], [7, 7], [1, 7], [1, 5]]]], 'Click', 42);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_array VALUES ([[[[5, 5], [5, -5], [-5, -5], [-5, 5]], [[1, 3], [1, 1], [3, 1], [3, -1], [1, -1], [1, -3], [-1, -3], [-1, -1], [-3, -1], [-3, 1], [-1, 1], [-1, 3]]]], 'House', 314159);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_array VALUES ([[[[3, 1], [0, 1], [0, -1], [3, -1]]]], 'Click East', 421);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_array VALUES ([[[[-1, 1], [1, 1], [1, 3], [-1, 3]]]], 'Click North', 422);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_array VALUES ([[[[-3, 1], [-3, -1], [0, -1], [0, 1]]]], 'Click South', 423);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_array VALUES ([[[[-1, -1], [1, -1], [1, -3], [-1, -3]]]], 'Click West', 424);"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_01037.polygons_tuple;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE test_01037.polygons_tuple (key Array(Array(Array(Tuple(Float64, Float64)))), name String, value UInt64) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_tuple VALUES ([[[(1, 3), (1, 1), (3, 1), (3, -1), (1, -1), (1, -3), (-1, -3), (-1, -1), (-3, -1), (-3, 1), (-1, 1), (-1, 3)]], [[(5, 5), (5, 1), (7, 1), (7, 7), (1, 7), (1, 5)]]], 'Click', 42);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_tuple VALUES ([[[(5, 5), (5, -5), (-5, -5), (-5, 5)], [(1, 3), (1, 1), (3, 1), (3, -1), (1, -1), (1, -3), (-1, -3), (-1, -1), (-3, -1), (-3, 1), (-1, 1), (-1, 3)]]], 'House', 314159);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_tuple VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Click East', 421);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_tuple VALUES ([[[(-1, 1), (1, 1), (1, 3), (-1, 3)]]], 'Click North', 422);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_tuple VALUES ([[[(-3, 1), (-3, -1), (0, -1), (0, 1)]]], 'Click South', 423);"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_tuple VALUES ([[[(-1, -1), (1, -1), (1, -3), (-1, -3)]]], 'Click West', 424);"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_01037.points;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE test_01037.points (x Float64, y Float64, def_i UInt64, def_s String) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (0.1, 0.0, 112, 'aax');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (-0.1, 0.0, 113, 'aay');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (0.0, 1.1, 114, 'aaz');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (0.0, -1.1, 115, 'aat');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (3.0, 3.0, 22, 'bb');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (5.0, 6.0, 33, 'cc');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (-100.0, -42.0, 44, 'dd');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (7.01, 7.01, 55, 'ee');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (0.99, 2.99, 66, 'ee');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (1.0, 0.0, 771, 'ffa');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (-1.0, 0.0, 772, 'ffb');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (0.0, 2.0, 773, 'ffc');"
$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points VALUES (0.0, -2.0, 774, 'ffd');"


declare -a SearchTypes=("POLYGON" "POLYGON_INDEX_EACH" "POLYGON_INDEX_CELL")

for type in ${SearchTypes[@]};
do
    $CLICKHOUSE_CLIENT --query="DROP DICTIONARY IF EXISTS test_01037.dict_array;"

    $CLICKHOUSE_CLIENT -n --query="
    CREATE DICTIONARY test_01037.dict_array
    (
    key Array(Array(Array(Array(Float64)))),
    name String DEFAULT 'qqq',
    value UInt64 DEFAULT 101
    )
    PRIMARY KEY key
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'polygons_array' PASSWORD '' DB 'test_01037'))
    LIFETIME(MIN 1 MAX 10)
    LAYOUT($type());
    "

    $CLICKHOUSE_CLIENT --query="DROP DICTIONARY IF EXISTS test_01037.dict_tuple;"

    $CLICKHOUSE_CLIENT -n --query="
    CREATE DICTIONARY test_01037.dict_tuple
    (
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String DEFAULT 'qqq',
    value UInt64 DEFAULT 101
    )
    PRIMARY KEY key
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'polygons_tuple' PASSWORD '' DB 'test_01037'))
    LIFETIME(MIN 1 MAX 10)
    LAYOUT($type());
    "

    outputFile="${TMP_DIR}/results${type}.out"

    $CLICKHOUSE_CLIENT -n --query="
    select 'dictGet', 'test_01037.dict_array' as dict_name, tuple(x, y) as key,
        dictGet(dict_name, 'name', key),
        dictGet(dict_name, 'value', key) from test_01037.points order by x, y;
    "  > $outputFile
    $CLICKHOUSE_CLIENT -n --query="
    select 'dictGetOrDefault', 'test_01037.dict_array' as dict_name, tuple(x, y) as key,
        dictGetOrDefault(dict_name, 'name', key, 'www'),
        dictGetOrDefault(dict_name, 'value', key, toUInt64(1234)) from test_01037.points order by x, y;
    "  >> $outputFile
    $CLICKHOUSE_CLIENT -n --query="
    select 'dictGetOrDefault', 'test_01037.dict_array' as dict_name, tuple(x, y) as key,
        dictGetOrDefault(dict_name, 'name', key, def_s),
        dictGetOrDefault(dict_name, 'value', key, def_i) from test_01037.points order by x, y;
    "  >> $outputFile
    $CLICKHOUSE_CLIENT -n --query="
    select 'dictGet', 'test_01037.dict_tuple' as dict_name, tuple(x, y) as key,
        dictGet(dict_name, 'name', key),
        dictGet(dict_name, 'value', key) from test_01037.points order by x, y;
    "  >> $outputFile
    $CLICKHOUSE_CLIENT -n --query="
    select 'dictGetOrDefault', 'test_01037.dict_tuple' as dict_name, tuple(x, y) as key,
        dictGetOrDefault(dict_name, 'name', key, 'www'),
        dictGetOrDefault(dict_name, 'value', key, toUInt64(1234)) from test_01037.points order by x, y;
    "  >> $outputFile
    $CLICKHOUSE_CLIENT -n --query="
    select 'dictGetOrDefault', 'test_01037.dict_tuple' as dict_name, tuple(x, y) as key,
        dictGetOrDefault(dict_name, 'name', key, def_s),
        dictGetOrDefault(dict_name, 'value', key, def_i) from test_01037.points order by x, y;
    "  >> $outputFile

    $CLICKHOUSE_CLIENT --query="
    select 'dictHas', 'test_01037.dict_array' as dict_name, tuple(x, y) as key,
        dictHas(dict_name, key) from test_01037.points order by x, y;
    "  >> $outputFile

    $CLICKHOUSE_CLIENT --query="
    select 'dictHas', 'test_01037.dict_tuple' as dict_name, tuple(x, y) as key,
        dictHas(dict_name, key) from test_01037.points order by x, y;
    "  >> $outputFile

    diff -q "${CURDIR}/01037_polygon_simple_test.ans" "$outputFile"
done

$CLICKHOUSE_CLIENT --query="DROP DICTIONARY test_01037.dict_array;"
$CLICKHOUSE_CLIENT --query="DROP DICTIONARY test_01037.dict_tuple;"
$CLICKHOUSE_CLIENT --query="DROP TABLE test_01037.polygons_array;"
$CLICKHOUSE_CLIENT --query="DROP TABLE test_01037.polygons_tuple;"
$CLICKHOUSE_CLIENT --query="DROP TABLE test_01037.points;"
$CLICKHOUSE_CLIENT --query="DROP DATABASE test_01037;"