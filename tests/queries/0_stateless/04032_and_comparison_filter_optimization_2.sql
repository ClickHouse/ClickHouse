SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;
SET optimize_empty_string_comparisons = 1;

DROP TABLE IF EXISTS 04032_t;

CREATE TABLE 04032_t
(
    i Int32,
    u UInt8,
    f Float64,
    s String,
    lc LowCardinality(String),
    dt DateTime('UTC')
)
ENGINE = Memory;

INSERT INTO 04032_t VALUES (1, 10, 1.5, 'a', 'x', '2024-01-01 00:00:00'), (3, 30, 3.0, 'c', 'y', '2024-06-15 12:00:00'), (5, 50, 5.5, 'e', 'z', '2025-01-01 00:00:00');

-- =====================================================================
-- Section 10: three+ filters on same expression
-- =====================================================================

-- a = 3 AND a > 1 AND a < 5 → prune range, keep a = 3 only
SELECT 'eq_range_prune';
SELECT * FROM 04032_t WHERE i = 3 AND i > 1 AND i < 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > 1 AND i < 5 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1 AND i < 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > 1 AND i < 5 SETTINGS optimize_redundant_comparisons = 1;

-- a > 1 AND a < 5 AND a > 3 → tighten to a > 3 AND a < 5
SELECT 'range_tighten';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND i > 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND i > 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND i > 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND i > 3 SETTINGS optimize_redundant_comparisons = 1;

-- a = 3 AND a < 5 AND a > 5 → conflict → CONSTANT 0
SELECT 'three_way_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i < 5 AND i > 5 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i < 5 AND i > 5 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 5 AND i > 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i < 5 AND i > 5 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 11: LowCardinality and String columns
-- =====================================================================

SELECT 'lc_eq_eq';
SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'y' ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'y' ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'y' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'y' SETTINGS optimize_redundant_comparisons = 1;

SELECT 'lc_eq_conflict';
SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'z' SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'z' SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'z' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE lc = 'y' AND lc = 'z' SETTINGS optimize_redundant_comparisons = 1;

SELECT 'str_eq_eq';
SELECT * FROM 04032_t WHERE s = 'c' AND s = 'c' ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE s = 'c' AND s = 'c' ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE s = 'c' AND s = 'c' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE s = 'c' AND s = 'c' SETTINGS optimize_redundant_comparisons = 1;

SELECT 'str_eq_conflict';
SELECT * FROM 04032_t WHERE s = 'c' AND s = 'a' SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE s = 'c' AND s = 'a' SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE s = 'c' AND s = 'a' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE s = 'c' AND s = 'a' SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 12: transitive inference + conflict detection (commit d4c0509a0c3)
-- =====================================================================

-- x = 3 AND x = y AND y = 5 → transitive infers x=5, then conflict x=3 vs x=5 → FALSE
SELECT 'transitive_conflict';
SELECT * FROM 04032_t WHERE i = 3 AND i = u AND u = 5 SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i = u AND u = 5 SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = u AND u = 5 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i = u AND u = 5 SETTINGS optimize_redundant_comparisons = 1;

-- x < 3 AND y > 3 AND y < 10 → should NOT produce redundant x < 10
SELECT 'transitive_no_redundant';
SELECT * FROM 04032_t WHERE i < 3 AND u > 3 AND u < 10 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i < 3 AND u > 3 AND u < 10 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND u > 3 AND u < 10 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i < 3 AND u > 3 AND u < 10 SETTINGS optimize_redundant_comparisons = 1;

-- a > b AND b > 3 → infers a > 3
SELECT 'transitive_infer';
SELECT * FROM 04032_t WHERE i > u AND u > 2 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > u AND u > 2 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > u AND u > 2 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > u AND u > 2 SETTINGS optimize_redundant_comparisons = 1;

-- chain: a = b AND b = 3 → infers a = 3
SELECT 'transitive_eq_chain';
SELECT * FROM 04032_t WHERE i = u AND u = 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = u AND u = 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = u AND u = 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = u AND u = 3 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 13: non-comparison operands preserved
-- =====================================================================

SELECT 'mixed_operands';
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND s != '' ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND s != '' ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND s != '' SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > 1 AND i < 5 AND s != '' SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 14: notEquals chain → NOT IN conversion
-- =====================================================================

SET optimize_min_inequality_conjunction_chain_length = 2;

SELECT 'ne_chain_not_in';
SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 7 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 7 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 7 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 7 SETTINGS optimize_redundant_comparisons = 1;

-- notEquals chain with redundant duplicate
SELECT 'ne_chain_dedup';
SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 1 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 1 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 1 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i != 1 AND i != 3 AND i != 1 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 15: edge cases
-- =====================================================================

-- single comparison (no optimization needed)
SELECT 'single';
SELECT * FROM 04032_t WHERE i = 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 SETTINGS optimize_redundant_comparisons = 1;

-- non-constant expression (not optimizable), WHERE = AND preserved
SELECT 'non_const';
SELECT * FROM 04032_t WHERE i > u AND i < u ORDER BY i SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i > u AND i < u ORDER BY i SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > u AND i < u SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i > u AND i < u SETTINGS optimize_redundant_comparisons = 1;

-- NULL handling: comparison with NULL should not crash
SELECT 'null_safe';
SELECT * FROM 04032_t WHERE i = 3 AND i > NULL SETTINGS optimize_redundant_comparisons = 0;
SELECT * FROM 04032_t WHERE i = 3 AND i > NULL SETTINGS optimize_redundant_comparisons = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > NULL SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM 04032_t WHERE i = 3 AND i > NULL SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 16: non-deterministic expressions must not be pruned
-- =====================================================================

-- Repeated rand() calls are independent at the AST level, so the conjunction
-- `rand() % 2 < 1 AND rand() % 2 >= 1` must NOT be folded to false. The rewritten plan
-- must keep both predicates and be identical for optimize_redundant_comparisons 0 and 1.
SELECT 'non_deterministic';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM numbers(1) WHERE rand() % 2 < 1 AND rand() % 2 >= 1 SETTINGS optimize_redundant_comparisons = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM numbers(1) WHERE rand() % 2 < 1 AND rand() % 2 >= 1 SETTINGS optimize_redundant_comparisons = 1;

-- Transitive inference must not cross non-deterministic expressions: from
-- `rand() % 2 = number % 2 AND rand() % 2 = 1` the chain pass must NOT derive
-- `number % 2 = 1` (the two rand() calls are independent), which would otherwise
-- conflict with `number % 2 = 0` and fold the whole AND to false.
SELECT 'non_deterministic_transitive';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM numbers(1) WHERE rand() % 2 = number % 2 AND rand() % 2 = 1 AND number % 2 = 0 SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM numbers(1) WHERE rand() % 2 = number % 2 AND rand() % 2 = 1 AND number % 2 = 0 SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;

-- =====================================================================
-- Section 17: NaN must not be pruned with range-ordering semantics
-- =====================================================================

-- `f < nan` is always false in query execution, so `f = 1 AND f < nan` is empty.
-- accurateLess orders NaN after ordinary values, so without a NaN guard the EQUALS
-- branch would treat accurateLess(1, nan) as true, prune `f < nan` and wrongly
-- return the f = 1 row. The result must be empty for both setting values.
SELECT 'nan_guard';
SELECT count() FROM (SELECT number::Float64 AS f FROM numbers(3)) WHERE f = 1 AND f < nan SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM (SELECT number::Float64 AS f FROM numbers(3)) WHERE f = 1 AND f < nan SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 18: notEquals chains vs equals/range predicates
-- =====================================================================
-- notEquals filters are kept in a per-expression ordered map (long machine-generated
-- exclusion chains stay linear), so cover the map-vs-range/equals transitions.

-- A range predicate prunes exactly the notEquals values it excludes.
SELECT 'range_prunes_not_equals';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number != 7 AND number != 8 AND number != 2 AND number < 5
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number != 7 AND number != 8 AND number != 2 AND number < 5 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number != 7 AND number != 8 AND number != 2 AND number < 5 SETTINGS optimize_redundant_comparisons = 1;

-- Inclusive boundary: `!= 5 AND <= 5` tightens to `< 5`, and the mirrored case.
SELECT 'boundary_strengthen';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number != 5 AND number <= 5
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number != 5 AND number <= 5 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number != 5 AND number <= 5 SETTINGS optimize_redundant_comparisons = 1;

SELECT 'boundary_strengthen_mirrored';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number >= 5 AND number != 5
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number >= 5 AND number != 5 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number >= 5 AND number != 5 SETTINGS optimize_redundant_comparisons = 1;

-- An equals predicate prunes every non-conflicting notEquals of the chain.
SELECT 'equals_prunes_chain';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number != 1 AND number != 2 AND number != 3 AND number = 7
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number != 1 AND number != 2 AND number != 3 AND number = 7 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number != 1 AND number != 2 AND number != 3 AND number = 7 SETTINGS optimize_redundant_comparisons = 1;

-- An equals predicate conflicting with one of the notEquals collapses the AND to false.
SELECT 'equals_conflicts_chain';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number != 1 AND number != 7 AND number != 3 AND number = 7
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number != 1 AND number != 7 AND number != 3 AND number = 7 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number != 1 AND number != 7 AND number != 3 AND number = 7 SETTINGS optimize_redundant_comparisons = 1;

-- Cross-representation duplicates dedup by converted value.
SELECT 'cross_type_dedup';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number != 3 AND number != 3.0 AND number != 3
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number != 3 AND number != 3.0 AND number != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number != 3 AND number != 3.0 AND number != 3 SETTINGS optimize_redundant_comparisons = 1;

-- Surviving chains still convert to NOT IN, keeping the original constant order.
SELECT 'chain_to_not_in';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number != 1 AND number != 3 AND number != 5 AND number != 7
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number != 1 AND number != 3 AND number != 5 AND number != 7 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number != 1 AND number != 3 AND number != 5 AND number != 7 SETTINGS optimize_redundant_comparisons = 1;

-- Range arriving before the chain: later notEquals outside the range are dropped on insertion.
SELECT 'range_first';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number < 5 AND number != 7 AND number != 2 AND number != 8
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number < 5 AND number != 7 AND number != 2 AND number != 8 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number < 5 AND number != 7 AND number != 2 AND number != 8 SETTINGS optimize_redundant_comparisons = 1;

-- Bool constants dedup across representations (true / 1 / 1.0 take different conversion paths).
SELECT 'bool_chain_dedup';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number % 2 = 0 AS b FROM numbers(4)) WHERE b != true AND b != 1 AND b != 1.0
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM (SELECT number % 2 = 0 AS b FROM numbers(4)) WHERE b != true AND b != 1 AND b != 1.0 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM (SELECT number % 2 = 0 AS b FROM numbers(4)) WHERE b != true AND b != 1 AND b != 1.0 SETTINGS optimize_redundant_comparisons = 1;

SELECT 'bool_equals_conflicts';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number % 2 = 0 AS b FROM numbers(4)) WHERE b = true AND b != 1.0
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM (SELECT number % 2 = 0 AS b FROM numbers(4)) WHERE b = true AND b != 1.0 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM (SELECT number % 2 = 0 AS b FROM numbers(4)) WHERE b = true AND b != 1.0 SETTINGS optimize_redundant_comparisons = 1;

-- An equals seen before the chain prunes each later notEquals on insertion.
SELECT 'equals_first';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number = 7 AND number != 1 AND number != 2
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number = 7 AND number != 1 AND number != 2 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number = 7 AND number != 1 AND number != 2 SETTINGS optimize_redundant_comparisons = 1;

-- NaN constants stay out of the analysis; the rest of the chain is still pruned.
SELECT 'nan_not_equals_kept';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number / 2. AS f FROM numbers(4)) WHERE f != nan AND f != 2 AND f < 1
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM (SELECT number / 2. AS f FROM numbers(4)) WHERE f != nan AND f != 2 AND f < 1 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM (SELECT number / 2. AS f FROM numbers(4)) WHERE f != nan AND f != 2 AND f < 1 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 19: AND reduced to a single wide-typed operand keeps boolean semantics
-- =====================================================================
-- `i < 300` is always true for Int8 and is pruned; the surviving operand must be
-- evaluated as a boolean (non-zero test), a plain cast would truncate 256::UInt32 to 0.

SELECT 'sole_survivor_uint32';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT toUInt32(number * 256) AS x, toInt8(number) AS i FROM numbers(3)) WHERE x AND i < 300
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM (SELECT toUInt32(number * 256) AS x, toInt8(number) AS i FROM numbers(3)) WHERE x AND i < 300 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM (SELECT toUInt32(number * 256) AS x, toInt8(number) AS i FROM numbers(3)) WHERE x AND i < 300 SETTINGS optimize_redundant_comparisons = 1;

-- The AND is also a value in a projection, it must evaluate to 0/1.
SELECT 'sole_survivor_projection';
SELECT x AND (i < 300) FROM (SELECT toUInt32(number * 256) AS x, toInt8(number) AS i FROM numbers(3)) ORDER BY x SETTINGS optimize_redundant_comparisons = 0;
SELECT x AND (i < 300) FROM (SELECT toUInt32(number * 256) AS x, toInt8(number) AS i FROM numbers(3)) ORDER BY x SETTINGS optimize_redundant_comparisons = 1;

SELECT 'sole_survivor_float';
SELECT count() FROM (SELECT number / 2. AS f, toInt8(number) AS i FROM numbers(3)) WHERE f AND i < 300 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM (SELECT number / 2. AS f, toInt8(number) AS i FROM numbers(3)) WHERE f AND i < 300 SETTINGS optimize_redundant_comparisons = 1;

-- Bool survivor still takes the lossless cast path.
SELECT 'sole_survivor_bool';
SELECT count() FROM (SELECT number % 2 = 0 AS b, toInt8(number) AS i FROM numbers(3)) WHERE b AND i < 300 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM (SELECT number % 2 = 0 AS b, toInt8(number) AS i FROM numbers(3)) WHERE b AND i < 300 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 20: transitive inference must not append implied conditions
-- =====================================================================

-- The nested `x < 2` implies the derived `x < 3`; the pruning pass sees only the direct
-- arguments of the outer AND, so the implied conjunct must not be appended at all.
SELECT 'derived_nested_subsumed';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10))
WHERE (x < 2 AND x < y) AND y < 3
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) WHERE (x < 2 AND x < y) AND y < 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) WHERE (x < 2 AND x < y) AND y < 3 SETTINGS optimize_redundant_comparisons = 1;

SELECT 'derived_flat_subsumed';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10))
WHERE x < 2 AND x < y AND y < 3
SETTINGS optimize_redundant_comparisons = 1;

-- The inference still works when it adds information: `x < 3` is derived and kept.
SELECT 'derived_kept';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10))
WHERE x < y AND y < 3
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM (SELECT number AS x, number * 2 AS y FROM numbers(10)) WHERE x < y AND y < 3 SETTINGS optimize_redundant_comparisons = 1;

-- Contradictory derived conditions still collapse the AND to false.
SELECT 'derived_conflict';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT number AS x, number AS y FROM numbers(10))
WHERE x = 3 AND x = y AND y = 5
SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 21: internal folds/rewrites materialize in the tree
-- =====================================================================
-- Even when nothing is pruned, the rewritten predicates must reach the AST.

SELECT 'rewrite_materialized_float';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number > 3.5 AND number < 8.5
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number > 3.5 AND number < 8.5 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number > 3.5 AND number < 8.5 SETTINGS optimize_redundant_comparisons = 1;

-- The AT_MAX fold (`>= 255` -> `= 255` for UInt8) materializes on the survivor.
SELECT 'rewrite_materialized_boundary';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT toUInt8(number) AS n FROM numbers(3)) WHERE n >= 255 AND n <= 255
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM (SELECT toUInt8(number) AS n FROM numbers(3)) WHERE n >= 255 AND n <= 255 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM (SELECT toUInt8(number) AS n FROM numbers(3)) WHERE n >= 255 AND n <= 255 SETTINGS optimize_redundant_comparisons = 1;

-- Size-preserving with a second column: the fold on `n` still materializes.
SELECT 'rewrite_materialized_two_columns';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM (SELECT toUInt8(number) AS n, toInt32(number) AS i FROM numbers(3)) WHERE n >= 255 AND i > 0
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM (SELECT toUInt8(number) AS n, toInt32(number) AS i FROM numbers(3)) WHERE n >= 255 AND i > 0 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM (SELECT toUInt8(number) AS n, toInt32(number) AS i FROM numbers(3)) WHERE n >= 255 AND i > 0 SETTINGS optimize_redundant_comparisons = 1;

-- =====================================================================
-- Section 22: a bound tightened by notEquals is rechecked against the other bound
-- =====================================================================

-- `<= 3` is tightened to `< 3` by `!= 3` and now contradicts `>= 3` → fold to false.
SELECT 'strengthen_then_conflict';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number <= 3 AND number >= 3 AND number != 3
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number <= 3 AND number >= 3 AND number != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number <= 3 AND number >= 3 AND number != 3 SETTINGS optimize_redundant_comparisons = 1;

SELECT 'strengthen_then_conflict_mirrored';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number >= 3 AND number <= 3 AND number != 3
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number >= 3 AND number <= 3 AND number != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number >= 3 AND number <= 3 AND number != 3 SETTINGS optimize_redundant_comparisons = 1;

-- Non-conflicting counterpart: the tightened interval stays valid.
SELECT 'strengthen_no_conflict';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT count() FROM numbers(10) WHERE number <= 3 AND number >= 1 AND number != 3
SETTINGS optimize_redundant_comparisons = 1;
SELECT count() FROM numbers(10) WHERE number <= 3 AND number >= 1 AND number != 3 SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM numbers(10) WHERE number <= 3 AND number >= 1 AND number != 3 SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE IF EXISTS 04032_t;
