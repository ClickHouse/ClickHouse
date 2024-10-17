#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS async_inserts_native;
    CREATE TABLE async_inserts_native (m Map(UInt64, UInt64), v UInt64 MATERIALIZED m[4]) ENGINE = Memory;
"

url="${CLICKHOUSE_URL}&async_insert=1&async_insert_busy_timeout_max_ms=1000&async_insert_busy_timeout_min_ms=1000&wait_for_async_insert=1"

# This test runs inserts with memory_tracker_fault_probability > 0 to trigger memory limit during insertion.
# If rollback of columns is wrong in that case it may produce LOGICAL_ERROR and it will caught by termintation of server in debug mode.
for _ in {1..10}; do
    ${CLICKHOUSE_CLIENT} -q "SELECT (range(number), range(number))::Map(UInt64, UInt64) AS m FROM numbers(1000) FORMAT Native" | \
        ${CLICKHOUSE_CURL} -sS -X POST "${url}&max_block_size=100&memory_tracker_fault_probability=0.01&query=INSERT+INTO+async_inserts_native+FORMAT+Native" --data-binary @- >/dev/null 2>&1 &
done

wait

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_inserts_native;"
