-- Regression for the out-of-bounds write into `partial_disjunction_result` when a query has nested
-- `coalesce`/`ifNull` comparisons with `allow_key_condition_coalesce_rewrite = 1` and
-- `use_skip_indexes_for_disjunctions = 1`.
--
-- Root cause (fixed in `tryRewriteCoalesceComparison`):
-- The first `cloneDAGWithInversionPushDown` pass rewrote the outer `coalesce(...) = const` into an
-- OR-tree of branches but added each per-branch `equals(y_i, const)` directly via `addFunction`,
-- skipping a recursive rewrite. When `y_i` itself was a `coalesce`, that inner `equals(coalesce, const)`
-- stayed unexpanded after the first pass. A second pass run by `MergeTreeIndexMinMax::createIndexCondition`
-- then expanded it, producing a per-index `KeyCondition` whose RPN length differed from (and exceeded)
-- the template's RPN length used by `MergeTreeDataSelectExecutor::filterMarksUsingIndex`. The bitset
-- `partial_disjunction_result` is sized as `marks_count * MAX_BITS_FOR_PARTIAL_DISJUNCTION_RESULT`,
-- so positions beyond `MAX_BITS_FOR_PARTIAL_DISJUNCTION_RESULT` (32) wrote out of bounds.

DROP TABLE IF EXISTS t_stid_3760;

CREATE TABLE t_stid_3760
(
    id UInt32,
    a Nullable(UInt32),
    b Nullable(UInt32),
    c Nullable(UInt32),
    d UInt32,
    INDEX a_idx a TYPE minmax GRANULARITY 1,
    INDEX b_idx b TYPE minmax GRANULARITY 1,
    INDEX c_idx c TYPE minmax GRANULARITY 1,
    INDEX d_idx d TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_stid_3760
SELECT
    number,
    if(number % 3 = 0, NULL, toUInt32(intDiv(number, 1024))),
    if(number % 5 = 0, NULL, toUInt32(intDiv(number, 1024) + 100)),
    if(number % 7 = 0, NULL, toUInt32(intDiv(number, 1024) + 200)),
    toUInt32(intDiv(number, 1024) + 1000000)
FROM numbers(65536);

-- Nested `coalesce`. Without the fix, the per-index `KeyCondition`'s RPN exceeds 32 elements and the
-- callback registered by `filterMarksUsingIndex` writes out of bounds, aborting the server.
SELECT count() FROM t_stid_3760
WHERE coalesce(a, b, c, coalesce(a, b, c, d), d) = 42
SETTINGS
    use_skip_indexes_on_data_read = 0,
    use_skip_indexes = 1,
    use_skip_indexes_for_disjunctions = 1,
    allow_key_condition_coalesce_rewrite = 1,
    use_query_condition_cache = 0;

-- Same predicate but reachable via the trivial-count / aggregate-projection optimizer path
-- (`optimizeUseAggregateProjections`), which is the path the AST fuzzer used to surface the bug.
SELECT trimBoth(explain) FROM (
    SELECT * FROM viewExplain('EXPLAIN', 'indexes = 1',
        (SELECT count() FROM t_stid_3760 WHERE coalesce(a, b, c, coalesce(a, b, c, d), d) = 42))
) WHERE explain ILIKE '%<Combined skip indexes>%'
SETTINGS
    use_skip_indexes_on_data_read = 0,
    use_skip_indexes = 1,
    use_skip_indexes_for_disjunctions = 1,
    allow_key_condition_coalesce_rewrite = 1,
    use_query_condition_cache = 0;

-- Triple-nested `coalesce`. The recursive rewrite must keep expanding each inner `equals(coalesce, const)`
-- until no further `coalesce` remains as the left-hand side of the comparison, otherwise the per-index
-- `KeyCondition` RPN diverges again from the template's.
SELECT count() FROM t_stid_3760
WHERE coalesce(a, coalesce(b, c, coalesce(a, b, c, d)), d) = 42
SETTINGS
    use_skip_indexes_on_data_read = 0,
    use_skip_indexes = 1,
    use_skip_indexes_for_disjunctions = 1,
    allow_key_condition_coalesce_rewrite = 1,
    use_query_condition_cache = 0;

-- `ifNull` follows the same rewrite path as `coalesce`; verify a mixed `coalesce`/`ifNull` nesting too.
SELECT count() FROM t_stid_3760
WHERE coalesce(a, b, ifNull(c, d), d) = 42
SETTINGS
    use_skip_indexes_on_data_read = 0,
    use_skip_indexes = 1,
    use_skip_indexes_for_disjunctions = 1,
    allow_key_condition_coalesce_rewrite = 1,
    use_query_condition_cache = 0;

-- Cross-check: result must equal the manual coalesce-to-OR expansion to confirm semantic equivalence.
SELECT count() FROM t_stid_3760
WHERE
       (a = 42)
    OR (a IS NULL AND b = 42)
    OR (a IS NULL AND b IS NULL AND c = 42)
    OR (a IS NULL AND b IS NULL AND c IS NULL AND
        ((a = 42)
      OR (a IS NULL AND b = 42)
      OR (a IS NULL AND b IS NULL AND c = 42)
      OR (a IS NULL AND b IS NULL AND c IS NULL AND d = 42)))
SETTINGS
    use_skip_indexes_on_data_read = 0,
    use_skip_indexes = 1,
    use_skip_indexes_for_disjunctions = 1,
    use_query_condition_cache = 0;

DROP TABLE t_stid_3760;
