#!/usr/bin/env bash
# Tags: no-replicated-database, no-random-merge-tree-settings
# no-random-merge-tree-settings: test requires deterministic index_granularity

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;

    DROP TABLE IF EXISTS t_hypo_stat;
    CREATE TABLE t_hypo_stat (a UInt64, b UInt64 STATISTICS(tdigest, uniq))
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

    -- 10000 rows: 100 granules of 100 rows each
    -- b cycles through 0..99 — 100 distinct values
    INSERT INTO t_hypo_stat SELECT number, number % 100 FROM numbers(10000);
"

# =========================================================
# Statistical: empirical disabled, statistical falls back to row selectivity
# Range query b < 50 — tdigest gives a stable ~50% selectivity estimate
# =========================================================
echo "--- statistical: range query on column with stats ---"
$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_stat (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF empirical = 0 SELECT * FROM t_hypo_stat WHERE b < 50;
" | grep -E '^\s+status:|^\s+source:|^\s+empirical_status:'

# =========================================================
# Statistical without column stats → falls through to applicability_only
# =========================================================
echo "--- statistical: no stats, falls back to applicability_only ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_no_stat;
    CREATE TABLE t_hypo_no_stat (a UInt64, b UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_hypo_no_stat SELECT number, number % 100 FROM numbers(10000);

    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_no_stat (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF empirical = 0 SELECT * FROM t_hypo_no_stat WHERE b < 50;
" | grep -E '^\s+status:|^\s+source:|^\s+empirical_status:'

# =========================================================
# Default (empirical=1) — empirical is still preferred when both are available
# =========================================================
echo "--- default: empirical preferred over statistical when both available ---"
$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_stat (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_stat WHERE b < 50;
" | grep -E '^\s+source:|^\s+empirical_status:'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_stat"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_no_stat"
