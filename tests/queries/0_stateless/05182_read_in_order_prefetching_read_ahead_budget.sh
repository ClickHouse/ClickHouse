#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `spreadMarkRangesAmongStreamsWithOrder` gives a stream inside the per-part `PrefetchingConcat`
# window the read-ahead budget a single merge input would have had from `BufferChunksTransform`:
# `read_in_order_use_buffering` switches it on, and its two halves are the step's own
# `block_size.max_block_size_rows` and `prefer_external_sort_block_bytes`. The rows half must come
# from the block size the step was built with rather than from a fresh `max_block_size` lookup,
# because a rebuilt step (the lazy-FINAL split, a shipped parallel-replicas fragment, a plain
# `clone`) carries the former and can be handed a context with a different latter.
#
# The budget is not visible in `EXPLAIN PIPELINE` (the processor is the same either way), so pin it
# from the reading step's own log line.

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_read_ahead_budget;

CREATE TABLE t_read_ahead_budget (key UInt64, value String)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_read_ahead_budget;

INSERT INTO t_read_ahead_budget SELECT number, toString(number) FROM numbers(5000);
INSERT INTO t_read_ahead_budget SELECT number, toString(number) FROM numbers(5000, 5000);
INSERT INTO t_read_ahead_budget SELECT number, toString(number) FROM numbers(10000, 5000);
"

CLICKHOUSE_CLIENT_TRACE_LOGS=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')

SETTINGS="--enable_parallel_replicas=0 --optimize_read_in_order=1 --max_threads=6 \
--merge_tree_min_rows_for_concurrent_read=1024 --merge_tree_min_bytes_for_concurrent_read=0 \
--merge_tree_min_read_task_size=2 --prefer_external_sort_block_bytes=1048576"

run_and_report()
{
    local label="$1"
    shift
    local log
    log=$(${CLICKHOUSE_CLIENT_TRACE_LOGS} ${SETTINGS} "$@" \
        -q "SELECT key FROM t_read_ahead_budget ORDER BY key FORMAT Null" 2>&1)
    echo "$label $(echo "$log" | grep -o "Using PrefetchingConcatProcessor for [0-9]* streams from part [^,]*, read-ahead budget [0-9]* rows / [0-9]* bytes" | sed 's/.*read-ahead budget //' | sort -u | paste -sd'|')"
}

# Buffering on: the rows half is the block size the step was built with, the bytes half is
# `prefer_external_sort_block_bytes`.
run_and_report buffering_on --read_in_order_use_buffering=1 --max_block_size=8192
# The rows half follows `max_block_size`, so a different block size gives a different budget.
run_and_report buffering_on_small_blocks --read_in_order_use_buffering=1 --max_block_size=4096
# Buffering off: no read-ahead at all, exactly as when the streams are separate merge inputs.
run_and_report buffering_off --read_in_order_use_buffering=0 --max_block_size=8192

# The budget must not change the answer.
$CLICKHOUSE_CLIENT ${SETTINGS} --read_in_order_use_buffering=0 -q "
SELECT count(), sum(key), groupArray(key) = arraySort(groupArray(key))
FROM (SELECT key FROM t_read_ahead_budget ORDER BY key);
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_read_ahead_budget;"
