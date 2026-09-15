#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_insert_queue_metrics (a UInt64) ENGINE = MergeTree ORDER BY a"

read_queue_bytes() {
    ${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.metrics WHERE metric = 'AsynchronousInsertQueueBytes'"
}

# Push more than a megabyte, so the leak this test guards against is far larger than the
# queue of any other test running at the same time.
push_big_insert() {
    seq 1 200000 | ${CLICKHOUSE_CLIENT} \
        --async_insert 1 \
        --wait_for_async_insert "$1" \
        --async_insert_use_adaptive_busy_timeout 0 \
        --async_insert_busy_timeout_min_ms "$2" \
        --async_insert_busy_timeout_max_ms "$2" \
        --async_insert_max_data_size 1000000000 \
        -q "INSERT INTO async_insert_queue_metrics FORMAT TSV"
}

# Flush by SYSTEM FLUSH ASYNC INSERT QUEUE.
before=$(read_queue_bytes)
push_big_insert 0 600000
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE async_insert_queue_metrics"
after=$(read_queue_bytes)
echo "leaked after explicit flush: $(( after - before > 100000 ))"

# Flush by the busy timeout.
before=$(read_queue_bytes)
push_big_insert 1 50
after=$(read_queue_bytes)
echo "leaked after timeout flush: $(( after - before > 100000 ))"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM async_insert_queue_metrics"

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_insert_queue_metrics"
