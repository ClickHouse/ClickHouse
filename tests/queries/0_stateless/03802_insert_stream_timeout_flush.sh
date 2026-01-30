#!/usr/bin/env bash
# Tags: no-async-insert, no-fasttest
# no-async-insert: Test expects new part for each time interval

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function run_test() {
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout SYNC"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_timeout (id UInt64, data String) ENGINE MergeTree ORDER BY id"

    {
        for iteration in 1 2; do
            for i in $(seq 1 40); do
                echo "{\"id\":$(( (iteration*100) + i )),\"data\":\"batch_${iteration}\"}"
            done

            sleep 2

            echo "{\"id\":$(( (iteration*100) + 99 )),\"data\":\"trigger_${iteration}\"}"
        done
    } | ${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_timeout FORMAT JSONEachRow" \
        --max_insert_block_size=1000 \
        --input_format_max_block_wait_ms=500 \
        --input_format_parallel_parsing=0 \
        --min_insert_block_size_bytes=1 \
	--max_query_size=1000 \
	--async_insert=0 \
	--max_insert_delayed_streams_for_parallel_write=0
      

    sleep 1
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log, part_log;"

    record_count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_insert_timeout")
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

    [[ "${parts_count}" -eq 2 ]] && return 0 || return 1
}

max_attempts=50
attempt=0

# This test depends on timeout-based behavior.
# Retry to avoid flakiness caused by timing variations under load.

while (( attempt < max_attempts )); do
    (( attempt++ ))
    if run_test; then
        break
    fi
    sleep 1
done

if (( attempt == max_attempts )); then
    echo "Test Failed After ${attempt}" >&2
    exit 1
fi


echo "Total records inserted: ${record_count}"
echo "Number of parts created: ${parts_count}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout"

