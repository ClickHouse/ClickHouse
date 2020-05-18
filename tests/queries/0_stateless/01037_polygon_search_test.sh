#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

TMP_DIR="/tmp"

declare -a SearchTypes=("POLYGON" "GRID_POLYGON" "BUCKET_POLYGON" "ONE_BUCKET_POLYGON")

unzip -q 01037_test_data.zip

$CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS test_01037;"

$CLICKHOUSE_CLIENT --query="CREATE DATABASE test_01037 Engine = Ordinary;"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_01037.points;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE test_01037.points (x Float64, y Float64) ENGINE = Memory;"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points FORMAT TSV" --max_insert_block_size=100000 < "./01037_point_data"

rm 01037_point_data

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

$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_array FORMAT JSONEachRow" --max_insert_block_size=100000 < "./01037_polygon_data"

rm 01037_polygon_data

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
    LAYOUT($type());"

    echo $type array finished

    $CLICKHOUSE_CLIENT -n --query="
    select 'dictGet', 'test_01037.dict_array' as dict_name, tuple(x, y) as key,
       dictGet(dict_name, 'name', key),
       dictGet(dict_name, 'value', key) from test_01037.points order by x, y;
    " > $TMP_DIR/results$type.out

    echo $type array query finished

    $CLICKHOUSE_CLIENT --query="DROP DICTIONARY test_01037.dict_array;"

    echo $type finished
done

$CLICKHOUSE_CLIENT --query="DROP TABLE test_01037.points;"
$CLICKHOUSE_CLIENT --query="DROP DATABASE test_01037;"

for ((i = 0; i < ${#SearchTypes[@]}; i++))
do
   for ((j = ($i + 1); j < ${#SearchTypes[@]}; j++))
   do
      type1="${TMP_DIR}/results${SearchTypes[$i]}.out"
      type2="${TMP_DIR}/results${SearchTypes[$j]}.out"
      if diff -q $type1 $type2; then
         echo "${SearchTypes[$i]} and ${SearchTypes[$j]} returned same results"
      else
         echo "Check diff in" ${SearchTypes[$i]}_diff_${SearchTypes[$j]}
         diff $type1 $type2 > $TMP_DIR/${SearchTypes[$i]}_diff_${SearchTypes[$j]}
      fi
   done
done