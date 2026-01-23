#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_timeout (id UInt64, data String) ENGINE MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES test_insert_timeout;"


{
    for i in $(seq 0 40); do
        echo "{\"id\":${i},\"data\":\"record_${i}\"}"
    done
    sleep 0.5

    for i in $(seq 41 59); do
        echo "{\"id\":${i},\"data\":\"record_${i}\"}"
    done
    sleep 0.5

    for i in $(seq 60 89); do
        echo "{\"id\":${i},\"data\":\"record_${i}\"}"
    done
} | ${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_timeout FORMAT JSONEachRow" \
    --max_insert_block_size=100 \
    --input_format_max_block_wait_ms=300 \
    --input_format_parallel_parsing=0 \
    --min_insert_block_size_bytes=1 \
    --max_query_size=1000

sleep 1
record_count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_insert_timeout")
echo "Total records inserted: ${record_count}"

parts_count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE table='test_insert_timeout' AND active AND database='${CLICKHOUSE_DATABASE}'")
echo "Number of parts created: ${parts_count}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout"

