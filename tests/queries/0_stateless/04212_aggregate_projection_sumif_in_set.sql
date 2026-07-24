-- Regression test: aggregate projection with multiple sumIf(..., col IN (...)) was broken.
--
-- Root cause: ColumnSet::operator[] returns an empty Field{} regardless of set contents,
-- so matchTrees (actionsDAGUtils.cpp) could not distinguish between different IN-clause
-- constants.  With two or more such aggregates in the projection all of them were mapped
-- to the first projection node that happened to win the parent-set iteration, causing:
--   * wrong results when both aggregates use the same column, or
--   * projection not used at all when they use different columns.
--
-- The fix adds a content-based, order-independent hash to FutureSetFromTuple and uses it
-- in the constant comparison instead of getField().

DROP TABLE IF EXISTS t_sumif_proj;

CREATE TABLE t_sumif_proj
(
    grp   UInt8,
    col   String,
    val1  UInt64,
    val2  UInt64,
    PROJECTION prj
    (
        SELECT grp, sumIf(val1, col IN ('a', 'b')) AS m1,
                     sumIf(val2, col IN ('c', 'd')) AS m2
        GROUP BY grp
    )
)
ENGINE = MergeTree
ORDER BY grp;

INSERT INTO t_sumif_proj
SELECT number % 4                        AS grp,
       ['a','b','c','d'][number % 4 + 1] AS col,
       number                            AS val1,
       number * 10                       AS val2
FROM numbers(40);

OPTIMIZE TABLE t_sumif_proj FINAL;

-- Helper: extract the ReadFromMergeTree line from EXPLAIN projections = 1
-- so the test shows clearly whether the projection or main table is read.

-- -----------------------------------------------------------------------
-- Repro 1: single aggregate — same IN order as projection
-- -----------------------------------------------------------------------
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp, sumIf(val1, col IN ('a', 'b')) AS m1
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1
) WHERE explain LIKE '%ReadFromMergeTree%';

-- Repro 1 — set reshuffle: reversed IN order must still hit the projection
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp, sumIf(val1, col IN ('b', 'a')) AS m1
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1
) WHERE explain LIKE '%ReadFromMergeTree%';

-- -----------------------------------------------------------------------
-- Repro 2: two aggregates, IN order matching projection — must use projection
-- -----------------------------------------------------------------------
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp,
           sumIf(val1, col IN ('a', 'b')) AS m1,
           sumIf(val2, col IN ('c', 'd')) AS m2
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1
) WHERE explain LIKE '%ReadFromMergeTree%';

-- -----------------------------------------------------------------------
-- Repro 2.1: two aggregates, reversed IN order — must still use projection
-- -----------------------------------------------------------------------
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp,
           sumIf(val1, col IN ('b', 'a')) AS m1,
           sumIf(val2, col IN ('d', 'c')) AS m2
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1
) WHERE explain LIKE '%ReadFromMergeTree%';

-- -----------------------------------------------------------------------
-- Repro 3: query exactly matching the projection definition — must use it
-- -----------------------------------------------------------------------
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp,
           sumIf(val1, col IN ('a', 'b')) AS m1,
           sumIf(val2, col IN ('c', 'd')) AS m2
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1
) WHERE explain LIKE '%ReadFromMergeTree%';

-- -----------------------------------------------------------------------
-- Correctness: results with projection must match results without it
-- -----------------------------------------------------------------------
SELECT grp,
       sumIf(val1, col IN ('a', 'b')) AS m1,
       sumIf(val2, col IN ('c', 'd')) AS m2
FROM t_sumif_proj
GROUP BY grp ORDER BY grp
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT grp,
       sumIf(val1, col IN ('b', 'a')) AS m1,
       sumIf(val2, col IN ('d', 'c')) AS m2
FROM t_sumif_proj
GROUP BY grp ORDER BY grp
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- -----------------------------------------------------------------------
-- `query_plan_max_set_size_for_projection_match` gates the content-hash
-- comparison: when the limit is smaller than the IN-set, the matcher must
-- treat the sets as non-equal and fall back to the base table. Setting it
-- to 0 disables the comparison entirely.
-- -----------------------------------------------------------------------

-- Limit = 1, set size = 2: projection must NOT be used.
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp, sumIf(val1, col IN ('a', 'b')) AS m1
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1, query_plan_max_set_size_for_projection_match = 1
) WHERE explain LIKE '%ReadFromMergeTree%';

-- Limit = 0 (disabled): projection must NOT be used.
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp, sumIf(val1, col IN ('a', 'b')) AS m1
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1, query_plan_max_set_size_for_projection_match = 0
) WHERE explain LIKE '%ReadFromMergeTree%';

-- Limit = 2 (exact size): projection must be used again.
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp, sumIf(val1, col IN ('a', 'b')) AS m1
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1, query_plan_max_set_size_for_projection_match = 2
) WHERE explain LIKE '%ReadFromMergeTree%';

-- Correctness: results with the comparison disabled must still match the
-- results computed with the projection.
SELECT grp,
       sumIf(val1, col IN ('a', 'b')) AS m1,
       sumIf(val2, col IN ('c', 'd')) AS m2
FROM t_sumif_proj
GROUP BY grp ORDER BY grp
SETTINGS optimize_use_projections = 1, query_plan_max_set_size_for_projection_match = 0;

-- -----------------------------------------------------------------------
-- Duplicate-values robustness. `Set::getTotalRowCount` is the deduplicated
-- size, so the size pre-filter must agree with the content hash on every
-- duplicate-input shape: equal sets despite duplicates ⇒ match, and equal
-- deduplicated sizes with differing contents ⇒ non-match (no false hit).
-- -----------------------------------------------------------------------

-- Duplicates that dedup to the projection's set: projection must be used.
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp, sumIf(val1, col IN ('a', 'a', 'b')) AS m1
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1
) WHERE explain LIKE '%ReadFromMergeTree%';

-- Same deduplicated size (2) as the projection but different contents:
-- the size check is ambiguous on its own, but the content hash forces
-- the matcher to reject. Projection must NOT be used.
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp, sumIf(val1, col IN ('a', 'c')) AS m1
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1
) WHERE explain LIKE '%ReadFromMergeTree%';

-- Different deduplicated size: projection must NOT be used (size early-exit).
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp, sumIf(val1, col IN ('a')) AS m1
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1
) WHERE explain LIKE '%ReadFromMergeTree%';

-- Duplicates that dedup to a different set than the projection: projection
-- must NOT be used.
SELECT trimLeft(explain) FROM (
    EXPLAIN projections = 1
    SELECT grp, sumIf(val1, col IN ('a', 'a', 'c')) AS m1
    FROM t_sumif_proj GROUP BY grp
    SETTINGS optimize_use_projections = 1
) WHERE explain LIKE '%ReadFromMergeTree%';

-- Correctness: result with the projection used (duplicates in the query) must
-- match the result computed against the base table.
SELECT grp, sumIf(val1, col IN ('a', 'a', 'b')) AS m1
FROM t_sumif_proj GROUP BY grp ORDER BY grp
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT grp, sumIf(val1, col IN ('a', 'a', 'b')) AS m1
FROM t_sumif_proj GROUP BY grp ORDER BY grp
SETTINGS optimize_use_projections = 0;

DROP TABLE t_sumif_proj;
