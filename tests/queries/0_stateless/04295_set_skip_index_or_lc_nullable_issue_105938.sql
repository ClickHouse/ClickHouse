-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105938
--
-- A `WHERE` clause that ORs two predicates over different skip indexes used to
-- silently return zero rows when the left-hand side hit a `set(N)` index on a
-- `LowCardinality(Nullable(String))` column whose granule values are all NULL,
-- and the right-hand side was an `ngrambf_v1` predicate on a different column.
--
-- Root cause: `MergeTreeIndexConditionSet::traverseDAG` wrapped every WHERE
-- atom with `__bitWrapperFunc`. For `LowCardinality(Nullable(String)) = '...'`
-- the atom result type is `LowCardinality(Nullable(UInt8))`. The pre-fix
-- wrapper coerced a NULL granule value to `0`, so the set index concluded the
-- granule definitely cannot match, and the ORed `ngrambf_v1` side was unable
-- to override the decision: the granule was incorrectly pruned.
--
-- The fix in https://github.com/ClickHouse/ClickHouse/pull/105384 detects that
-- the atom result type is not integer (after stripping `Nullable`) and falls
-- back to `UNKNOWN_FIELD`, so the set index does not attempt to prune the
-- granule and the query returns the same result as with `use_skip_indexes = 0`.

DROP TABLE IF EXISTS t_105938;

CREATE TABLE t_105938
(
    s String,
    n LowCardinality(Nullable(String)),
    INDEX idx_n n TYPE set(100) GRANULARITY 1,
    INDEX idx_s s TYPE ngrambf_v1(4, 1024, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_105938 SELECT 'has-republisher', NULL FROM numbers(100);
INSERT INTO t_105938 SELECT 'no-match',        NULL FROM numbers(10);

-- All three expressions must agree: 100 matching rows.

SELECT count() FROM t_105938
WHERE (n = 'republisher' OR s LIKE '%republisher%');

SELECT count() FROM t_105938
WHERE (n = 'republisher' OR s LIKE '%republisher%')
SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_105938
WHERE (assumeNotNull(n) = 'republisher' OR s LIKE '%republisher%');

DROP TABLE t_105938;
