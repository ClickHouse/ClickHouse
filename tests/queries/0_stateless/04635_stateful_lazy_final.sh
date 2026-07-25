#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The lazy FINAL optimization (query_plan_optimize_lazy_final) builds a key set on a hidden
# non-FINAL read that replays the WHERE / PREWHERE / row-level filters, and then evaluates the
# same filters again on the result branch. A stateful predicate must run exactly once, on the
# row stream of the query as written, so `optimizeLazyFinal` must bail out for stateful filters:
#   - `neighbor` over the pre-FINAL duplicate stream selects a different key set than over the
#     FINAL stream, so the optimized query would return different rows;
#   - `logTrace` would emit its message once per block of the hidden set-building read on top of
#     the per-block messages of the actual query.

TABLE="t_04635"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS $TABLE;
    CREATE TABLE $TABLE (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k
        SETTINGS index_granularity = 1;
    SYSTEM STOP MERGES $TABLE;
    INSERT INTO $TABLE SELECT number, number FROM numbers(10);
    INSERT INTO $TABLE SELECT number, number + 100 FROM numbers(10);
"

COMMON_SETTINGS="max_threads = 1, max_block_size = 65536, enable_parallel_replicas = 0, allow_deprecated_error_prone_window_functions = 1"

# Value-based check: the FINAL stream is v = 100..109, so `neighbor(v, 1) = 101` matches exactly
# the first row. Without the stateful bailout, the key set is built from the pre-FINAL stream
# (where the old part's rows v = 0..9 fail the predicate), the read is narrowed to key 0 only,
# and the replayed filter over that single row sees `neighbor` = 0 - returning 0 rows.
for lazy in 0 1; do
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM $TABLE FINAL WHERE neighbor(v, 1) = 101
        SETTINGS $COMMON_SETTINGS, query_plan_optimize_lazy_final = $lazy;
    "
done

# Side-effect check: an explicit stateful PREWHERE must log the same number of messages with the
# lazy FINAL optimization enabled as with it disabled. Without the bailout, the cloned prewhere
# on the hidden set-building read logs additionally for each of its blocks.
count_log_lines()
{
    $CLICKHOUSE_CLIENT --send_logs_level=trace -q "
        SELECT count() FROM $TABLE FINAL PREWHERE logTrace('lzf04635') = 0
        SETTINGS $COMMON_SETTINGS, query_plan_optimize_lazy_final = $1;
    " 2>&1 | grep -c "lzf04635"
}

lines_off=$(count_log_lines 0)
lines_on=$(count_log_lines 1)

if [ "$lines_on" -eq "$lines_off" ] && [ "$lines_off" -ge 1 ]; then
    echo "OK"
else
    echo "FAIL: $lines_off log lines without lazy FINAL, $lines_on with it"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE $TABLE"
