#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest, no-parallel-replicas
# no-parallel-replicas: ORDER BY ALL is missing, but this test doesn't add additional value with parallel replicas enabled, so skip it
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Parquet"

# File generated with the below script

#import pyarrow as pa
#import pyarrow.parquet as pq
#import random
#
#
#def gen_array(offset):
#	array = []
#	array_length = random.randint(0, 9)
#	for i in range(array_length):
#		array.append(i + offset)
#
#	return array
#
#
#def gen_arrays(number_of_arrays):
#	list_of_arrays = []
#	for i in range(number_of_arrays):
#		list_of_arrays.append(gen_array(i))
#	return list_of_arrays
#
#arr = pa.array(gen_arrays(70000))
#table  = pa.table([arr], ["arr"])
#pq.write_table(table, "int-list-zero-based-chunked-array.parquet")

DATA_FILE=$CUR_DIR/data_parquet/int-list-zero-based-chunked-array.parquet
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_load"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_load (arr Array(Int64)) ENGINE = Memory"
cat "$DATA_FILE" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO parquet_load FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_load SETTINGS max_threads=1" | md5sum
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM parquet_load"
${CLICKHOUSE_CLIENT} --query="drop table parquet_load"
