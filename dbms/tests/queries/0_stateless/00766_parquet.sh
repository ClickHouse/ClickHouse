#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CUR_DIR/../shell_config.sh

#${CLICKHOUSE_CLIENT} --max_block_size=1 --query="SELECT * FROM system.numbers LIMIT 10 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t1.pq
#${CLICKHOUSE_CLIENT} --max_block_size=5 --query="SELECT * FROM system.numbers LIMIT 10 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t5.pq
#${CLICKHOUSE_CLIENT} --max_block_size=15 --query="SELECT * FROM system.numbers LIMIT 10 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t15.pq
#${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 100000 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t100000.pq
#${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 1000000000 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t1g.pq
#valgrind --tool=massif  ${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 1000000 FORMAT Parquet" > ${CLICKHOUSE_TMP}/t1g.pq


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.contributors"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.contributors (name String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.contributors ORDER BY name DESC FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.contributors FORMAT Parquet"
# random results
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.contributors LIMIT 10" > /dev/null
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.contributors"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.parquet_numbers"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.parquet_numbers (number UInt64) ENGINE = Memory"
# less than default block size (65k)
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 10000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.parquet_numbers ORDER BY number DESC LIMIT 10"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE test.parquet_numbers"

# More than default block size
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 100000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.parquet_numbers ORDER BY number DESC LIMIT 10"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE test.parquet_numbers"

#${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 10000000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.parquet_numbers FORMAT Parquet"
#${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.parquet_numbers ORDER BY number DESC LIMIT 10"
#${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE test.parquet_numbers"

${CLICKHOUSE_CLIENT} --max_block_size=2 --query="SELECT * FROM system.numbers LIMIT 3 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.parquet_numbers ORDER BY number DESC LIMIT 10"

${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE test.parquet_numbers"
${CLICKHOUSE_CLIENT} --max_block_size=1 --query="SELECT * FROM system.numbers LIMIT 1000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.parquet_numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.parquet_numbers ORDER BY number DESC LIMIT 10"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.parquet_numbers"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.parquet_events"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.parquet_events (event String, value UInt64, description String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.events FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.parquet_events FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT event, description FROM test.parquet_events WHERE event IN ('ContextLock', 'Query') ORDER BY event"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.parquet_events"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.parquet_types1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.parquet_types2"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.parquet_types1       (int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32, uint32 UInt32, int64 Int64, uint64 UInt64, float32 Float32, float64 Float64, string String, fixedstring FixedString(15), date Date, datetime DateTime) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.parquet_types2       (int8 Int8, uint8 UInt8, int16 Int16, uint16 UInt16, int32 Int32, uint32 UInt32, int64 Int64, uint64 UInt64, float32 Float32, float64 Float64, string String, fixedstring FixedString(15), date Date, datetime DateTime) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.parquet_types1 values (     -108,         108,       -1016,          1116,       -1032,          1132,       -1064,          1164,          -1.032,          -1.064,      'string', 'fixedstring', '2000-01-01', '2018-01-01 01:01:01')"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.parquet_types1 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.parquet_types2 FORMAT Parquet"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.parquet_types1"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.parquet_types2"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.parquet_types1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.parquet_types2"
