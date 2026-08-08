#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "--- projection-served query is rejected ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_proj;
    CREATE TABLE t_hypo_proj (a UInt64, b UInt64, PROJECTION p (SELECT a, b ORDER BY b))
    ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_hypo_proj SELECT number, number FROM numbers(1000);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_proj (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT a FROM t_hypo_proj WHERE b = 5 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
" 2>&1 | grep -m1 -o 'served from a projection'


echo "--- empty table reports a clean baseline ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_empty;
    CREATE TABLE t_hypo_empty (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_empty (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_empty WHERE b = 42;
" | grep -E '^  parts:|^With |^\s+status:|^\s+reason:'


echo "--- old analyzer: empirical estimate works ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_old;
    CREATE TABLE t_hypo_old (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100;
    INSERT INTO t_hypo_old SELECT number, number FROM numbers(10000);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_old (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_old WHERE b = 42 SETTINGS enable_analyzer = 0;
" | grep -E '^With |^\s+status:|^\s+source:'

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_proj;
    DROP TABLE IF EXISTS t_hypo_empty;
    DROP TABLE IF EXISTS t_hypo_old;
"
