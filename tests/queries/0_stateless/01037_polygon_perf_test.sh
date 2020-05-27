#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

TMP_DIR="/tmp"

declare -a SearchTypes=("POLYGON_INDEX_EACH" "POLYGON_INDEX_CELL")

tar -xf ${CURDIR}/01037_test_data_perf.tar.gz

$CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS test_01037;"

$CLICKHOUSE_CLIENT --query="CREATE DATABASE test_01037 Engine = Ordinary;"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_01037.points;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE test_01037.points (x Float64, y Float64) ENGINE = Memory;"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points FORMAT TSV" --max_insert_block_size=100000 < "${CURDIR}/01037_point_data"

rm ${CURDIR}/01037_point_data

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_01037.polygons_array;"

$CLICKHOUSE_CLIENT --query="
CREATE TABLE test_01037.polygons_array
(
   key Array(Array(Array(Array(Float64)))),
   name String,
   value UInt64
)
ENGINE = Memory;
"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_array FORMAT JSONEachRow" --max_insert_block_size=100000 < "${CURDIR}/01037_polygon_data"

rm ${CURDIR}/01037_polygon_data

$CLICKHOUSE_CLIENT --query="DROP DICTIONARY IF EXISTS test_01037.dict_array;"

for type in ${SearchTypes[@]};
do 
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
   LAYOUT($type());"

   outputFile="${TMP_DIR}/results${type}.out"

   $CLICKHOUSE_CLIENT -n --query="
   select 'dictGet', 'test_01037.dict_array' as dict_name, tuple(x, y) as key,
      dictGet(dict_name, 'value', key) from test_01037.points order by x, y;
   " > $outputFile

   diff -q "${CURDIR}/01037_polygon_perf_test.ans" "$outputFile"

   $CLICKHOUSE_CLIENT --query="DROP DICTIONARY test_01037.dict_array;"
done

$CLICKHOUSE_CLIENT --query="DROP TABLE test_01037.points;"
$CLICKHOUSE_CLIENT --query="DROP DATABASE test_01037;"