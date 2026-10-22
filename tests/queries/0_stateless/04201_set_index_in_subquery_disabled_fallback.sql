-- Test: graceful fallback when set skip index cannot build subquery set
-- Covers: src/Storages/MergeTree/MergeTreeIndexSet.cpp:369-370
--         if (!set->buildOrderedSetInplace(context)) return;
-- Trigger: indexHint(col IN (subquery)) with use_index_for_in_with_subqueries=0
-- so FutureSetFromSubquery::buildOrderedSetInplace returns nullptr and the
-- set index condition is gracefully skipped (full scan) rather than crashing.

DROP TABLE IF EXISTS t_04201;

CREATE TABLE t_04201 (k UInt64, v UInt64, INDEX i (v) TYPE set(100) GRANULARITY 2)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity=8192, index_granularity_bytes=1000000000, min_index_granularity_bytes=0, add_minmax_index_for_numeric_columns=0;

INSERT INTO t_04201 SELECT number, intDiv(number, 4096) FROM numbers(1000000) SETTINGS optimize_trivial_insert_select=1;

-- With setting=0: subquery set cannot be built, index gracefully skipped → full scan (1000000 rows).
SELECT sum(1+ignore(*)) FROM t_04201 WHERE indexHint(v IN (SELECT 20)) SETTINGS use_index_for_in_with_subqueries=0;

-- With setting=1: subquery set built, index used → 2 granules at granularity 2 = 16384 rows.
SELECT sum(1+ignore(*)) FROM t_04201 WHERE indexHint(v IN (SELECT 20)) SETTINGS use_index_for_in_with_subqueries=1;

DROP TABLE t_04201;
