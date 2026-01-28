#!/usr/bin/env bash
# Tags: no-async-insert
# no-async-insert: Test expects new part for each time interval


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_timeout (id UInt64, data String) ENGINE MergeTree ORDER BY id"


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
    --input_format_max_block_wait_ms=200 \
    --input_format_parallel_parsing=0 \
    --min_insert_block_size_bytes=1 \
    --max_query_size=1000

sleep 1
record_count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_insert_timeout")
echo "Total records inserted: ${record_count}"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, part_log;"

parts_count=$(${CLICKHOUSE_CLIENT} --query "
SELECT count(*) 
FROM system.part_log 
WHERE table = 'test_insert_timeout' 
  AND event_type = 'NewPart'
  AND query_id = (
        SELECT argMax(query_id, event_time) 
        FROM system.query_log 
        WHERE query LIKE '%INSERT INTO test_insert_timeout%' 
          AND current_database = currentDatabase()
    )
")


echo "Number of parts created: ${parts_count}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout"

