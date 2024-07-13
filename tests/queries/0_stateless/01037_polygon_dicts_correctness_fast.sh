#!/usr/bin/env bash
# Tags: no-debug

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TMP_DIR="/tmp"

declare -a SearchTypes=("POLYGON_INDEX_EACH" "POLYGON_INDEX_CELL")

tar -xf "${CURDIR}"/01037_test_data_perf.tar.gz -C "${CURDIR}"

$CLICKHOUSE_CLIENT -n --query="
CREATE TABLE points (x Float64, y Float64) ENGINE = Memory;
"

$CLICKHOUSE_CLIENT --query="INSERT INTO points FORMAT TSV" --min_chunk_bytes_for_parallel_parsing=10485760 --max_insert_block_size=100000 < "${CURDIR}/01037_point_data"

rm "${CURDIR}"/01037_point_data

$CLICKHOUSE_CLIENT -n --query="
DROP TABLE IF EXISTS polygons_array;

CREATE TABLE polygons_array
(
   key Array(Array(Array(Array(Float64)))),
   name String,
   value UInt64
)
ENGINE = Memory;
"

$CLICKHOUSE_CLIENT --query="INSERT INTO polygons_array FORMAT JSONEachRow" --min_chunk_bytes_for_parallel_parsing=10485760 --max_insert_block_size=100000 < "${CURDIR}/01037_polygon_data"

rm "${CURDIR}"/01037_polygon_data

for type in "${SearchTypes[@]}";
do
   outputFile="${TMP_DIR}/results${type}.out"

   $CLICKHOUSE_CLIENT -n --query="
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

   select 'dictGet', 'dict_array' as dict_name, tuple(x, y) as key,
      dictGet(dict_name, 'value', key) from points order by x, y;
   " > "$outputFile"

   diff -q "${CURDIR}/01037_polygon_dicts_correctness_fast.ans" "$outputFile"
done

