#!/usr/bin/env bash

# `query_plan_optimize_lazy_final` built its winner-selection primary-key `Set` with
# `transform_null_in` = false, so a NULL key was never inserted and the NULL-keyed FINAL
# winner was pruned away and silently missing from the result.
#
# Every result cell compares the lazy plan against the same query with the optimization
# off, so a cell reads 1 only when both plans agree. The fixture makes the FINAL winner
# itself satisfy the filter (flag = 0 carries the highest version): propagating the
# pre-FINAL filter or not then yields the same winner set, so only null handling can
# differ here. Each key is spread over two parts so the parts intersect and the lazy
# branch is really used, which the two 'lazy FINAL executes' cells assert for the
# Nullable(UInt64) and LowCardinality(Nullable(String)) shapes.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The lazy FINAL branch is not built by the old analyzer, so without this the whole
# test is silently vacuous under compatibility randomization and on the old-analyzer job.
settings="--optimize_on_insert=1"

# Settings that select the lazy branch, shared by every cell that must take it.
lazy="query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0"

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_null_u64;
    DROP TABLE IF EXISTS t_null_str;
    DROP TABLE IF EXISTS t_null_lc;
    DROP TABLE IF EXISTS t_null_2nd;
    DROP TABLE IF EXISTS t_nonnull;
    DROP TABLE IF EXISTS t_null_f64;
    DROP TABLE IF EXISTS t_plain_f64;
    DROP TABLE IF EXISTS t_null_absent;
    DROP TABLE IF EXISTS t_arr_null;
    DROP TABLE IF EXISTS t_null_skip;

    -- 1. bare Nullable(UInt64) primary key holding a NULL
    CREATE TABLE t_null_u64 (k Nullable(UInt64), flag UInt8, version UInt64)
    ENGINE = ReplacingMergeTree(version) ORDER BY k
    SETTINGS index_granularity = 1, allow_nullable_key = 1;
    SYSTEM STOP MERGES t_null_u64;
    INSERT INTO t_null_u64 VALUES (5, 1, 1), (NULL, 1, 1);
    INSERT INTO t_null_u64 VALUES (5, 0, 2), (NULL, 0, 2);

    -- 2. Nullable(String) primary key holding a NULL
    CREATE TABLE t_null_str (k Nullable(String), flag UInt8, version UInt64)
    ENGINE = ReplacingMergeTree(version) ORDER BY k
    SETTINGS index_granularity = 1, allow_nullable_key = 1;
    SYSTEM STOP MERGES t_null_str;
    INSERT INTO t_null_str VALUES ('a', 1, 1), (NULL, 1, 1);
    INSERT INTO t_null_str VALUES ('a', 0, 2), (NULL, 0, 2);

    -- 3. LowCardinality(Nullable(String)). This cell is the reason the fix tests
    -- \`isNullableOrLowCardinalityNullable\`: \`DataTypeLowCardinality::isNullable\` returns
    -- false, so a plain \`isNullable\` predicate leaves this shape broken while cells 1, 2
    -- and 4 pass.
    CREATE TABLE t_null_lc (k LowCardinality(Nullable(String)), flag UInt8, version UInt64)
    ENGINE = ReplacingMergeTree(version) ORDER BY k
    SETTINGS index_granularity = 1, allow_nullable_key = 1;
    SYSTEM STOP MERGES t_null_lc;
    INSERT INTO t_null_lc VALUES ('a', 1, 1), (NULL, 1, 1);
    INSERT INTO t_null_lc VALUES ('a', 0, 2), (NULL, 0, 2);

    -- 4. two-column primary key with the NULL in the second component
    CREATE TABLE t_null_2nd (a UInt64, k Nullable(UInt64), flag UInt8, version UInt64)
    ENGINE = ReplacingMergeTree(version) ORDER BY (a, k)
    SETTINGS index_granularity = 1, allow_nullable_key = 1;
    SYSTEM STOP MERGES t_null_2nd;
    INSERT INTO t_null_2nd VALUES (1, 5, 1, 1), (1, NULL, 1, 1);
    INSERT INTO t_null_2nd VALUES (1, 5, 0, 2), (1, NULL, 0, 2);

    -- 5. control: a non-nullable primary key. Correct before the fix, and it is what
    -- makes the cells above specific to nullability rather than to the fixture shape.
    CREATE TABLE t_nonnull (k UInt64, flag UInt8, version UInt64)
    ENGINE = ReplacingMergeTree(version) ORDER BY k
    SETTINGS index_granularity = 1;
    SYSTEM STOP MERGES t_nonnull;
    INSERT INTO t_nonnull VALUES (5, 1, 1), (7, 1, 1);
    INSERT INTO t_nonnull VALUES (5, 0, 2), (7, 0, 2);

    -- 6. Nullable(Float64) primary key holding a NULL
    CREATE TABLE t_null_f64 (k Nullable(Float64), flag UInt8, version UInt64)
    ENGINE = ReplacingMergeTree(version) ORDER BY k
    SETTINGS index_granularity = 1, allow_nullable_key = 1;
    SYSTEM STOP MERGES t_null_f64;
    INSERT INTO t_null_f64 VALUES (5.5, 1, 1), (NULL, 1, 1);
    INSERT INTO t_null_f64 VALUES (5.5, 0, 2), (NULL, 0, 2);

    -- 7. control: a plain Float64 key holding nan, correct before the fix and unchanged by it.
    -- A nan key sorts like a NULL one but is not one, so this cell documents that a non-nullable
    -- float key needs no special handling. It cannot detect an over-generalized predicate: with no
    -- top-level Nullable in the key, no null map is built and no key is skipped either way.
    CREATE TABLE t_plain_f64 (k Float64, flag UInt8, version UInt64)
    ENGINE = ReplacingMergeTree(version) ORDER BY k
    SETTINGS index_granularity = 1;
    SYSTEM STOP MERGES t_plain_f64;
    INSERT INTO t_plain_f64 VALUES (5.5, 1, 1), (nan, 1, 1);
    INSERT INTO t_plain_f64 VALUES (5.5, 0, 2), (nan, 0, 2);

    -- 8. control: a Nullable(UInt64) key whose data holds no NULL, correct before the fix.
    -- It separates \"the key type admits NULL\" from \"a NULL is present\": only the latter broke.
    CREATE TABLE t_null_absent (k Nullable(UInt64), flag UInt8, version UInt64)
    ENGINE = ReplacingMergeTree(version) ORDER BY k
    SETTINGS index_granularity = 1, allow_nullable_key = 1;
    SYSTEM STOP MERGES t_null_absent;
    INSERT INTO t_null_absent VALUES (5, 1, 1), (7, 1, 1);
    INSERT INTO t_null_absent VALUES (5, 0, 2), (7, 0, 2);

    -- 9. control: Array(Nullable(UInt64)). A NULL nested inside a container is correct before
    -- the fix and stays correct after it, because the winner set only ever builds a null map
    -- from a top-level Nullable key, so this cell pins that a container key is left alone.
    CREATE TABLE t_arr_null (k Array(Nullable(UInt64)), flag UInt8, version UInt64)
    ENGINE = ReplacingMergeTree(version) ORDER BY k
    SETTINGS index_granularity = 1, allow_nullable_key = 1;
    SYSTEM STOP MERGES t_arr_null;
    INSERT INTO t_arr_null VALUES ([1], 1, 1), ([NULL], 1, 1);
    INSERT INTO t_arr_null VALUES ([1], 0, 2), ([NULL], 0, 2);

    -- 10. Nullable(UInt64) key with a set skip index, which evaluates the winner-set
    -- predicate on stored key values.
    CREATE TABLE t_null_skip (k Nullable(UInt64), flag UInt8, version UInt64, INDEX ik k TYPE set(0) GRANULARITY 1)
    ENGINE = ReplacingMergeTree(version) ORDER BY k
    SETTINGS index_granularity = 1, allow_nullable_key = 1;
    SYSTEM STOP MERGES t_null_skip;
    INSERT INTO t_null_skip VALUES (5, 1, 1), (NULL, 1, 1);
    INSERT INTO t_null_skip VALUES (5, 0, 2), (NULL, 0, 2);
"

$CLICKHOUSE_CLIENT $settings -q "
    SELECT 'Nullable(UInt64)',
        (SELECT arraySort(groupArray((k, flag, version))) FROM t_null_u64 FINAL PREWHERE flag = 0
            SETTINGS $lazy)
      = (SELECT arraySort(groupArray((k, flag, version))) FROM t_null_u64 FINAL PREWHERE flag = 0
            SETTINGS query_plan_optimize_lazy_final = 0);

    SELECT 'Nullable(String)',
        (SELECT arraySort(groupArray((k, flag, version))) FROM t_null_str FINAL PREWHERE flag = 0
            SETTINGS $lazy)
      = (SELECT arraySort(groupArray((k, flag, version))) FROM t_null_str FINAL PREWHERE flag = 0
            SETTINGS query_plan_optimize_lazy_final = 0);

    SELECT 'LowCardinality(Nullable(String))',
        (SELECT arraySort(groupArray((k, flag, version))) FROM t_null_lc FINAL PREWHERE flag = 0
            SETTINGS $lazy)
      = (SELECT arraySort(groupArray((k, flag, version))) FROM t_null_lc FINAL PREWHERE flag = 0
            SETTINGS query_plan_optimize_lazy_final = 0);

    SELECT 'NULL in second key component',
        (SELECT arraySort(groupArray((a, k, flag, version))) FROM t_null_2nd FINAL PREWHERE flag = 0
            SETTINGS $lazy)
      = (SELECT arraySort(groupArray((a, k, flag, version))) FROM t_null_2nd FINAL PREWHERE flag = 0
            SETTINGS query_plan_optimize_lazy_final = 0);

    SELECT 'control: non-nullable key',
        (SELECT arraySort(groupArray((k, flag, version))) FROM t_nonnull FINAL PREWHERE flag = 0
            SETTINGS $lazy)
      = (SELECT arraySort(groupArray((k, flag, version))) FROM t_nonnull FINAL PREWHERE flag = 0
            SETTINGS query_plan_optimize_lazy_final = 0);

    SELECT 'Nullable(Float64)',
        (SELECT arraySort(groupArray((toString(k), flag, version))) FROM t_null_f64 FINAL PREWHERE flag = 0
            SETTINGS $lazy)
      = (SELECT arraySort(groupArray((toString(k), flag, version))) FROM t_null_f64 FINAL PREWHERE flag = 0
            SETTINGS query_plan_optimize_lazy_final = 0);

    SELECT 'control: plain Float64 with nan',
        (SELECT arraySort(groupArray((toString(k), flag, version))) FROM t_plain_f64 FINAL PREWHERE flag = 0
            SETTINGS $lazy)
      = (SELECT arraySort(groupArray((toString(k), flag, version))) FROM t_plain_f64 FINAL PREWHERE flag = 0
            SETTINGS query_plan_optimize_lazy_final = 0);

    SELECT 'control: nullable key without a NULL value',
        (SELECT arraySort(groupArray((k, flag, version))) FROM t_null_absent FINAL PREWHERE flag = 0
            SETTINGS $lazy)
      = (SELECT arraySort(groupArray((k, flag, version))) FROM t_null_absent FINAL PREWHERE flag = 0
            SETTINGS query_plan_optimize_lazy_final = 0);

    SELECT 'control: Array(Nullable(UInt64))',
        (SELECT arraySort(groupArray((toString(k), flag, version))) FROM t_arr_null FINAL PREWHERE flag = 0
            SETTINGS $lazy)
      = (SELECT arraySort(groupArray((toString(k), flag, version))) FROM t_arr_null FINAL PREWHERE flag = 0
            SETTINGS query_plan_optimize_lazy_final = 0);

    SELECT 'Nullable(UInt64) with a set skip index',
        (SELECT arraySort(groupArray((k, flag, version))) FROM t_null_skip FINAL PREWHERE flag = 0
            SETTINGS $lazy)
      = (SELECT arraySort(groupArray((k, flag, version))) FROM t_null_skip FINAL PREWHERE flag = 0
            SETTINGS query_plan_optimize_lazy_final = 0);
"

# The ten cells above compare the optimization against itself being disabled, so they all
# still read 1 if the optimization silently stops applying at runtime. This cell asserts
# only that the step is PLANNED; the two cells after it assert that it EXECUTED.
$CLICKHOUSE_CLIENT $settings -q "
    SELECT 'lazy FINAL is planned for a nullable key',
        countIf(explain ILIKE '%LazyReadReplacingFinal%')
    FROM (
        EXPLAIN indexes = 0 SELECT k FROM t_null_u64 FINAL PREWHERE flag = 0
        SETTINGS $lazy
    );
"

# Runtime liveness. \`LazyMaterializingTransform\` logs this line from
# \`LazyMaterializingRows::filterRangesAndFillRows\`, and the only
# \`LazyMaterializingRows\` this plan can build is created inside the lazy branch of
# \`optimizeLazyFinal\`, so a non-zero count means that branch really ran. A fallback to
# the non-lazy branch computes the same correct answer, so the ten result cells cannot
# detect it and this cell is what does.
echo -n 'lazy FINAL executes for Nullable(UInt64) '
$CLICKHOUSE_CLIENT $settings -q "
    SELECT k FROM t_null_u64 FINAL PREWHERE flag = 0
    SETTINGS $lazy
" --send_logs_level='trace' 2>&1 \
    | grep -c 'LazyMaterializingTransform.*Lazily reading'

# `LowCardinality(Nullable(String))` is the shape only the wrapper-aware predicate fixes,
# so a fallback specific to it is exactly the regression this cell must catch.
echo -n 'lazy FINAL executes for LowCardinality(Nullable(String)) '
$CLICKHOUSE_CLIENT $settings -q "
    SELECT k FROM t_null_lc FINAL PREWHERE flag = 0
    SETTINGS $lazy
" --send_logs_level='trace' 2>&1 \
    | grep -c 'LazyMaterializingTransform.*Lazily reading'

# `set_rows` is the row count of the winner-selection `Set` that this fix builds, logged by
# `LazyFinalKeyAnalysisTransform` immediately after it ran primary-key index analysis with that
# set. It reads 2 only when the NULL key was inserted and 1 when it was dropped, so it is the
# only cell that observes the fixed value directly rather than by comparing the optimization
# against itself. It pins the set's CONTENTS, not that index analysis consumed the set: the set
# is filled by a separate `CreatingSetStep` beforehand, so `set_rows` is unchanged if
# consumption breaks. That property is pinned by `03990_lazy_final_index_analysis.sh`, which
# asserts exact selected marks.
echo -n 'lazy FINAL set holds the NULL key '
$CLICKHOUSE_CLIENT $settings -q "
    SELECT k FROM t_null_u64 FINAL PREWHERE flag = 0
    SETTINGS $lazy
" --send_logs_level='trace' 2>&1 \
    | grep -oE 'Lazy FINAL enabled:.*set_rows=[0-9]+' \
    | grep -oE 'set_rows=[0-9]+' \
    | head -1

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE t_null_u64;
    DROP TABLE t_null_str;
    DROP TABLE t_null_lc;
    DROP TABLE t_null_2nd;
    DROP TABLE t_nonnull;
    DROP TABLE t_null_f64;
    DROP TABLE t_plain_f64;
    DROP TABLE t_null_absent;
    DROP TABLE t_arr_null;
    DROP TABLE t_null_skip;
"
