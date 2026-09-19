#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel-replicas
# no-replicated-database: hypothetical indexes are session-scoped and not replicated

# Test that `EXPLAIN WHATIF` estimates a hypothetical `minmax` index on an expression the query
# analyzer renames the same way the real read path uses it: the estimator and the read path share
# the rewrite-aware index condition. Regression test for issue #103128.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_whatif_rewrites;
    CREATE TABLE t_whatif_rewrites (t UInt32, v Int32) ENGINE = MergeTree ORDER BY t
    SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
    INSERT INTO t_whatif_rewrites SELECT number, number % 100 FROM numbers(100);
"

echo "--- hypothetical minmax index on a rewritten expression ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_multiif ON t_whatif_rewrites (multiIf(v > 0, v, NULL)) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT t FROM t_whatif_rewrites WHERE multiIf(v > 0, v, NULL) > 97;
" | grep -E '^With |^\s+status:|^\s+marks:'

echo "--- the same estimate with the rewrite disabled ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_multiif ON t_whatif_rewrites (multiIf(v > 0, v, NULL)) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT t FROM t_whatif_rewrites WHERE multiIf(v > 0, v, NULL) > 97 SETTINGS optimize_multiif_to_if = 0;
" | grep -E '^With |^\s+status:|^\s+marks:'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_whatif_rewrites;"
