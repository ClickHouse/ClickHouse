#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-random-merge-tree-settings
# no-fasttest: column statistics (tdigest/uniq) require the full build
# no-replicated-database: hypothetical indexes are session-scoped and not replicated
# no-random-merge-tree-settings: test requires deterministic index_granularity

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;
    SET materialize_statistics_on_insert = 1;

    DROP TABLE IF EXISTS t_hypo_stat_mc;
    CREATE TABLE t_hypo_stat_mc (a UInt64, b UInt64 STATISTICS(tdigest, uniq), c UInt64 STATISTICS(tdigest, uniq))
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '';
    INSERT INTO t_hypo_stat_mc SELECT number, number % 100, number % 50 FROM numbers(10000);
"

# Statistical path is used only when the filter touches just the index's columns.
echo "--- statistical: filter on index column only ---"
$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_stat_mc (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF empirical = 0 SELECT * FROM t_hypo_stat_mc WHERE b < 50;
" | grep -E '^\s+status:|^\s+source:|^\s+empirical_status:'

# Filter references a non-index column (c), so statistical estimation bails to applicability_only.
echo "--- statistical: filter touches non-index column, bails to applicability_only ---"
$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_stat_mc (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF empirical = 0 SELECT * FROM t_hypo_stat_mc WHERE b < 50 AND c > 10;
" | grep -E '^\s+status:|^\s+source:|^\s+empirical_status:'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_stat_mc"
