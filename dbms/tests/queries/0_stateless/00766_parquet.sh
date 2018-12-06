#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CUR_DIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.contributors"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.contributors (name String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.contributors ORDER BY name DESC FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.contributors FORMAT Parquet"
# random results
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.contributors LIMIT 10" > /dev/null
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.contributors"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.numbers"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.numbers (number UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 65000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.numbers FORMAT Parquet"
#TODO fix it with 65k+ limit: Code: 33. DB::Exception: Error while reading parquet data: IOError: Unexpected end of stream: Page was smaller (4277) than expected (524288) 

#${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.numbers ORDER BY number DESC LIMIT 10 " > ${CLICKHOUSE_TMP}/00766_parquet.numbers
#diff $CUR_DIR/00766_parquet.reference ${CLICKHOUSE_TMP}/00766_parquet.numbers
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.numbers ORDER BY number DESC LIMIT 10 " 

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.numbers"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.events"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.events (event String, value UInt64, description String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.events FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.events FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT event, description FROM test.events WHERE event IN ('ContextLock', 'Query') ORDER BY event"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.events"
