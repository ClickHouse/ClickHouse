-- `optimize_redundant_comparisons` prunes a comparison that another comparison on the same expression
-- implies. It decides that with `FieldAccurateComparison`, a total order that places `NaN` after every
-- ordinary value. A comparison is evaluated in that order only where it ends in `IColumn::compareAt`;
-- a pair of top-level `Tuple`s and a pair of `Array`s with no least supertype are instead decomposed
-- into per-element applications of the comparison function, where a comparison against a `NaN` is
-- false and nothing is ordered. Placing such a comparison on the order therefore pruned a bound that
-- excludes a row, and the row was returned.
--
-- Both settings are pinned on every query: the runner randomizes them, an unpinned
-- `optimize_redundant_comparisons` makes the result arms vacuous, and `optimize_and_compare_chain`
-- adds `indexHint` operands that move the node counts.

SET enable_analyzer = 1;

-- 1) A pair of `Array` bounds with no least supertype (`UInt64` against `Int64`).
--    The two single-condition counts are the ground truth: one bound matches the row, the other does
--    not, so the conjunction matches nothing.
DROP TABLE IF EXISTS t_element_wise_array;
CREATE TABLE t_element_wise_array (id UInt32, a Array(Tuple(Float64, UInt64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_array VALUES (1, [(nan, toUInt64(9))]);
SELECT count() FROM t_element_wise_array WHERE a >= [(1., toUInt64(1))] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;
SELECT count() FROM t_element_wise_array WHERE a >= [(0., toInt64(0))] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;
SELECT count() FROM t_element_wise_array WHERE a >= [(1., toUInt64(1))] AND a >= [(0., toInt64(0))] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0;
SELECT count() FROM t_element_wise_array WHERE a >= [(1., toUInt64(1))] AND a >= [(0., toInt64(0))] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;

-- 2) A pair of top-level `Tuple`s. `executeTuple` is reached before the equal-types shortcut, so the
--    top level is decomposed even here, where the two constants differ only one level down.
DROP TABLE IF EXISTS t_element_wise_tuple;
CREATE TABLE t_element_wise_tuple (id UInt32, tu Tuple(Array(Tuple(Float64, UInt64)))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_tuple VALUES (1, tuple([(nan, toUInt64(9))]));
SELECT count() FROM t_element_wise_tuple WHERE tu >= tuple([(1., toUInt64(1))]) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;
SELECT count() FROM t_element_wise_tuple WHERE tu >= tuple([(0., toInt64(0))]) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;
SELECT count() FROM t_element_wise_tuple WHERE tu >= tuple([(1., toUInt64(1))]) AND tu >= tuple([(0., toInt64(0))]) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0;
SELECT count() FROM t_element_wise_tuple WHERE tu >= tuple([(1., toUInt64(1))]) AND tu >= tuple([(0., toInt64(0))]) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;

-- 3) A constant `String` against a `Tuple` column. `executeWithConstString` converts the literal to
--    the column's type and re-enters the dispatch, so the comparison is decomposed even though the
--    constant is declared a `String`.
DROP TABLE IF EXISTS t_element_wise_const_string;
CREATE TABLE t_element_wise_const_string (id UInt32, tu Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_const_string VALUES (1, (3., 3.));
SELECT count() FROM t_element_wise_const_string WHERE tu = '(3,3)' SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;
SELECT count() FROM t_element_wise_const_string WHERE tu <= '(nan,9)' SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;
SELECT count() FROM t_element_wise_const_string WHERE tu = '(3,3)' AND tu <= '(nan,9)' SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0;
SELECT count() FROM t_element_wise_const_string WHERE tu = '(3,3)' AND tu <= '(nan,9)' SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;

-- The result arms above also pass if the optimization stops running altogether, so pin what still has
-- to be pruned. Both the enabled and the disabled count are pinned: a relative comparison also holds
-- if the predicates disappear entirely.
DROP TABLE IF EXISTS t_element_wise_live;
CREATE TABLE t_element_wise_live (id UInt32, a Array(Tuple(Float64, UInt64)), m Map(String, Float64), i UInt32, j Int32, tu Tuple(Int64, Int64), af Array(Float64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_live VALUES (1, [(1., toUInt64(2))], map('k', 3.), 3, 3, (3, 3), [1.]);

-- 4) A same-type `Array` pair has a least supertype, is compared through `compareAt` and keeps folding.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE a = [(1., toUInt64(2))] AND a >= [(0., toUInt64(0))] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE a = [(1., toUInt64(2))] AND a >= [(0., toUInt64(0))] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';

-- 5) A `Map` is never decomposed, so it keeps folding too.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE m = map('k', 3.) AND m >= map('k', 0.) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE m = map('k', 3.) AND m >= map('k', 0.) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';

-- 6) A scalar column with a differently-typed constant keeps folding: the restriction is to containers.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE i = 3 AND i < toUInt32(5) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: less,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE i = 3 AND i < toUInt32(5) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: less,%';

-- 7) A scalar column with a constant `String` keeps folding: the literal is converted to a scalar type.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE j = 3 AND j < '5' SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: less,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE j = 3 AND j < '5' SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: less,%';

-- 8) The fold given up: a `Tuple` pair is decomposed whatever its element types, so both bounds stay.
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE tu = (3, 3) AND tu <= (9, 9) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: lessOrEquals,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE tu = (3, 3) AND tu <= (9, 9) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: lessOrEquals,%';

-- 9) Two `Array` types that differ but have a least supertype are compared through `compareAt`, so this
--    fold has to survive as well. Testing type equality instead of the supertype would give up this one.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE af = [1.] AND af >= CAST([0.], 'Array(Float32)') SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_live WHERE af = [1.] AND af >= CAST([0.], 'Array(Float32)') SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';

DROP TABLE t_element_wise_array;
DROP TABLE t_element_wise_tuple;
DROP TABLE t_element_wise_const_string;
DROP TABLE t_element_wise_live;
