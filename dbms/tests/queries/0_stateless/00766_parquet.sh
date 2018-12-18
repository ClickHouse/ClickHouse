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

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.numbers"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.numbers (number UInt64) ENGINE = Memory"
# less than default block size (65k)
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 10000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.numbers ORDER BY number DESC LIMIT 10"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE test.numbers"

# More than default block size
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 100000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.numbers ORDER BY number DESC LIMIT 10"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE test.numbers"

#${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 10000000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.numbers FORMAT Parquet"
#${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.numbers ORDER BY number DESC LIMIT 10"
#${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE test.numbers"

${CLICKHOUSE_CLIENT} --max_block_size=2 --query="SELECT * FROM system.numbers LIMIT 3 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.numbers ORDER BY number DESC LIMIT 10"

${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE test.numbers"
${CLICKHOUSE_CLIENT} --max_block_size=1 --query="SELECT * FROM system.numbers LIMIT 1000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.numbers FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.numbers ORDER BY number DESC LIMIT 10"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.numbers"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.events (event String, value UInt64, description String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.events FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.events FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT event, description FROM test.events WHERE event IN ('ContextLock', 'Query') ORDER BY event"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.events"

