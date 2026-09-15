#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_insert_flush_event (a UInt64) ENGINE = MergeTree ORDER BY a"

# The counter is global and other queries on the server may increment it, so compare the delta with the lower bound.
flushes_before=$(${CLICKHOUSE_CLIENT} -q "SELECT sum(value) FROM system.events WHERE event = 'AsyncInsertFlush'")

for _ in {1..3}; do
    ${CLICKHOUSE_CLIENT} --async_insert 1 --wait_for_async_insert 1 --async_insert_busy_timeout_min_ms 10 --async_insert_busy_timeout_max_ms 100 \
        -q "INSERT INTO async_insert_flush_event VALUES (1)"
done

flushes_after=$(${CLICKHOUSE_CLIENT} -q "SELECT sum(value) FROM system.events WHERE event = 'AsyncInsertFlush'")

# Every insert waits for its own flush, so at least three flushes must be counted.
${CLICKHOUSE_CLIENT} -q "SELECT ${flushes_after} - ${flushes_before} >= 3"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM async_insert_flush_event"

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_insert_flush_event"
