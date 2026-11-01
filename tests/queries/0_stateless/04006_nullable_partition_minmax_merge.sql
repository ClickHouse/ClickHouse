-- Tags: no-random-merge-tree-settings

-- Regression test for MinMaxIndex::merge with Nullable partition keys.
--
-- MinMaxIndex::merge used std::min/std::max which rely on Field::operator<.
-- Field::operator< compares type IDs first, and Null (type ID 0) is always
-- "less than" any other type, so POSITIVE_INFINITY (a Null sentinel used for
-- NULL-last extremes) compares as less than any real value. This corrupts
-- min/max bounds when merging a part containing NULLs with another part.
--
-- The fix replaces std::min/std::max with accurateLess which handles Nullable
-- fields correctly, consistent with MinMaxIndex::update.
--
-- To trigger: use PARTITION BY coalesce(b, 0) where b is Nullable. This makes
-- the partition key a non-nullable expression, but the MinMaxIndex tracks the
-- underlying column b (which IS Nullable). Rows with b=NULL and b=0 land in
-- the same partition (coalesce maps both to 0), but their MinMaxIndex entries
-- for b differ: [+INF, +INF] vs [0, 0]. Merging these with std::min/std::max
-- produces wrong bounds.

DROP TABLE IF EXISTS t_nullable_minmax_merge;

CREATE TABLE t_nullable_minmax_merge
(
    b Nullable(Int64),
    value String
)
ENGINE = MergeTree()
ORDER BY tuple()
PARTITION BY coalesce(b, 0)
SETTINGS allow_nullable_key = 1;

-- Part 1: b=NULL → partition value = coalesce(NULL,0) = 0
--   MinMaxIndex for column b: min=+INF, max=+INF (all NULLs, NullLast)
INSERT INTO t_nullable_minmax_merge VALUES (NULL, 'null_row');

-- Part 2: b=0 → partition value = coalesce(0,0) = 0 (same partition!)
--   MinMaxIndex for column b: min=0, max=0
INSERT INTO t_nullable_minmax_merge VALUES (0, 'zero_row');

-- OPTIMIZE FINAL merges the two parts in the same partition.
-- MinMaxIndex::merge is called. With the bug (std::min/std::max):
--   std::min(+INF_field, 0_field): +INF has Null type_id=0 < Int64 type_id=2
--     → returns +INF (WRONG, should be 0)
--   std::max(+INF_field, 0_field): same comparison
--     → returns 0 (WRONG, should be +INF)
-- Merged bounds become [+INF, 0] which is inverted/nonsensical.
OPTIMIZE TABLE t_nullable_minmax_merge FINAL;

-- Verify data correctness after merge
SELECT b, value FROM t_nullable_minmax_merge ORDER BY b NULLS LAST;

-- Test partition pruning. With corrupted minmax bounds, this could give
-- wrong results or miss rows.
SELECT value FROM t_nullable_minmax_merge WHERE b = 0;
SELECT value FROM t_nullable_minmax_merge WHERE b IS NULL;

DROP TABLE t_nullable_minmax_merge;
