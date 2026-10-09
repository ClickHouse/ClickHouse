#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_hypo_reason;
    CREATE TABLE t_hypo_reason (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_hypo_reason SELECT number, number % 100 FROM numbers(10000);
"

# A non-zero seek gap makes a real read coalesce ranges, so the per-granule count cannot model it
# and the empirical tier is skipped. The output must say why instead of silently degrading
echo "--- a skipped empirical tier says why ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_reason (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_reason WHERE b = 42 SETTINGS merge_tree_min_rows_for_seek = 1;
" 2>&1 | grep -E '^  (empirical_status|empirical_reason):' | awk '{$1=$1; print}'

echo "--- with defaults the empirical tier runs and there is no reason to give ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_reason (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_reason WHERE b = 42;
" 2>&1 | grep -E '^  (empirical_status|empirical_reason):' | awk '{$1=$1; print}'

# a query fully pruned by the primary key leaves an empty baseline, which is another path that
# skips the empirical tier and so must also say why
echo "--- an empty baseline also says why ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi_b ON t_hypo_reason (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_hypo_reason WHERE a = 999999999 AND b = 42;
" 2>&1 | grep -E '^  (empirical_status|empirical_reason):' | awk '{$1=$1; print}'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_reason;"
