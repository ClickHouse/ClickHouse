#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-random-merge-tree-settings
# no-fasttest: column statistics (tdigest/uniq) require the full build
# no-replicated-database: hypothetical indexes are session-scoped and not replicated
# no-random-merge-tree-settings: test requires deterministic index_granularity

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FAILPOINT=merge_tree_load_statistics_throw
cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FAILPOINT}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

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

# The failpoint is installed only in the no-argument loadStatistics() overload.
# WhatIf must request the filter columns explicitly, so an unrelated failure in
# an unrestricted all-columns load cannot disable the statistical estimate.
echo "--- statistical: required-column load bypasses unrestricted statistics failpoint ---"
$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;
    CREATE HYPOTHETICAL INDEX idx_b_failpoint ON t_hypo_stat_mc (b) TYPE minmax GRANULARITY 1;
    SYSTEM ENABLE FAILPOINT ${FAILPOINT};
    EXPLAIN WHATIF empirical = 0 SELECT * FROM t_hypo_stat_mc WHERE b < 50;
    SYSTEM DISABLE FAILPOINT ${FAILPOINT};
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

DROP_TABLE=t_hypo_stat_nullable
$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;
    SET materialize_statistics_on_insert = 1;

    DROP TABLE IF EXISTS ${DROP_TABLE};
    CREATE TABLE ${DROP_TABLE}
    (
        id UInt64,
        v Nullable(UInt64) STATISTICS(basic, tdigest),
        m Nullable(UInt64) STATISTICS(basic, tdigest)
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '';
    INSERT INTO ${DROP_TABLE}
    SELECT number,
           if(number % 4 = 0, NULL, toUInt64(number % 100)),
           if(number % 2 = 0, NULL, toUInt64(number % 100))
    FROM numbers(10000);
    ALTER TABLE ${DROP_TABLE} ADD COLUMN \`m.null\` UInt8 DEFAULT 1;
"

# The analyzer exposes the synthetic `v.null` input while statistics are keyed
# by `v`. Matching may resolve only this Nullable null subcolumn to its parent.
echo "--- statistical: synthetic nullable .null input maps to parent ---"
$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;
    CREATE HYPOTHETICAL INDEX idx_v_null_synthetic ON ${DROP_TABLE} (v) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF empirical = 0 SELECT * FROM ${DROP_TABLE} WHERE v < 50 AND NOT v.null;
" | grep -E '^\s+status:|^\s+source:|^\s+empirical_status:'

# An index explicitly defined on the synthetic subcolumn was already accepted
# by exact-name matching. Keep that behavior before falling back to the parent.
echo "--- statistical: exact synthetic .null index remains applicable ---"
$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;
    CREATE HYPOTHETICAL INDEX idx_v_null_exact ON ${DROP_TABLE} (v.null) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF empirical = 0 SELECT * FROM ${DROP_TABLE} WHERE NOT v.null;
" | grep -E '^\s+status:|^\s+source:|^\s+empirical_status:'

# A physical top-level `m.null` added after the part was written shadows the
# synthetic subcolumn. Even though the old part schema does not contain it, the
# current schema must prevent borrowing the unrelated parent `m` statistics.
echo "--- applicability_only: physical .null shadow has no statistics ---"
$CLICKHOUSE_CLIENT -n -q "
    SET allow_experimental_statistics = 1;
    SET allow_statistics_optimize = 1;
    CREATE HYPOTHETICAL INDEX idx_m_null_physical ON ${DROP_TABLE} (\`m.null\`) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF empirical = 0 SELECT * FROM ${DROP_TABLE} WHERE \`m.null\`;
" | grep -E '^\s+status:|^\s+source:|^\s+empirical_status:'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DROP_TABLE}"
