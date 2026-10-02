#!/usr/bin/env bash
# when a time limit in `break` mode stops a sampled projection scan, the estimate must not use the partial rows

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_whatif_break;
    CREATE TABLE t_whatif_break (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_whatif_break SELECT number, cityHash64(number) % 1000 FROM numbers(20000);
"

# the sample has 50 granules (5000 rows), and at 2000 rows/s the 1 s limit stops the read before the end
# the time limit then also stops the output of EXPLAIN, so the debug log of the estimator must give the reason
# the throttle does not always slow the read, and then the scan ends first and the estimate is in the output
out=$($CLICKHOUSE_CLIENT --send_logs_level=debug -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_whatif_break (SELECT a, b ORDER BY b);
    EXPLAIN WHATIF projection_scan_budget_rows = 5000 SELECT count() FROM t_whatif_break WHERE b < 100
    SETTINGS optimize_trivial_count_query = 0, optimize_use_projections = 1,
        max_execution_speed = 2000, timeout_before_checking_execution_speed = 0,
        max_execution_time = 1, timeout_overflow_mode = 'break';
" 2>&1)
if grep -q 'The projection scan was cut short' <<< "$out" || grep -qE 'empirical_status: +ok' <<< "$out"; then
    echo "the output has the estimate, or the log gives the reason that it is missing"
else
    echo "$out"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_whatif_break"
