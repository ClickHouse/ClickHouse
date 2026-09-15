#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_insert_queue_metrics (s String) ENGINE = MergeTree ORDER BY tuple()"

# Both metrics must describe the queue that system.asynchronous_inserts reports: its
# 'total_bytes' is the same per-entry size the metric accumulates, and it has one row per
# queue entry. Comparing the metrics against that queue, instead of against an earlier
# reading, is what makes this test immune to the inserts of tests running in parallel,
# because both sides of the comparison move together. The margins absorb the entries such
# a test can push between the two reads; a leak here is a whole megabyte.
queue_metrics_differ() {
    ${CLICKHOUSE_CLIENT} -q "
        SELECT
            abs((SELECT value FROM system.metrics WHERE metric = 'AsynchronousInsertQueueBytes')
                - (SELECT sum(total_bytes) FROM system.asynchronous_inserts)) > 100000,
            abs((SELECT value FROM system.metrics WHERE metric = 'AsynchronousInsertQueueSize')
                - (SELECT count() FROM system.asynchronous_inserts)) > 10
        FORMAT TSV"
}

# One row of a megabyte keeps the insert cheap and the leak unmistakable.
payload=$(head -c 1000000 /dev/zero | tr '\0' 'a')

# Flush by SYSTEM FLUSH ASYNC INSERT QUEUE. Nothing may flush on its own: the data stays
# below the size limit and the busy timeout never elapses.
echo "$payload" | ${CLICKHOUSE_CLIENT} \
    --async_insert 1 \
    --wait_for_async_insert 0 \
    --async_insert_use_adaptive_busy_timeout 0 \
    --async_insert_busy_timeout_min_ms 600000 \
    --async_insert_busy_timeout_max_ms 600000 \
    --async_insert_max_data_size 1000000000 \
    -q "INSERT INTO async_insert_queue_metrics FORMAT TSV"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE async_insert_queue_metrics"
echo -n "differ after explicit flush: "; queue_metrics_differ

# Flush by the data size limit, which happens inside the push itself. That path discounted
# the entry before this fix too, so here it guards against discounting it twice.
echo "$payload" | ${CLICKHOUSE_CLIENT} \
    --async_insert 1 \
    --wait_for_async_insert 1 \
    --async_insert_use_adaptive_busy_timeout 0 \
    --async_insert_busy_timeout_min_ms 600000 \
    --async_insert_busy_timeout_max_ms 600000 \
    --async_insert_max_data_size 1024 \
    -q "INSERT INTO async_insert_queue_metrics FORMAT TSV"
echo -n "differ after size flush: "; queue_metrics_differ

${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(length(s)) FROM async_insert_queue_metrics"

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_insert_queue_metrics"
