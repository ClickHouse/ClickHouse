-- `optimize_redundant_comparisons` prunes a comparison implied by another using a total order over the
-- constants, and two things break that order: a comparison decomposed into per-element ones (a
-- top-level `Tuple` pair, an `Array` pair with no least supertype) is not ordered by it at all, and a
-- `NULL` nested in a container is ordered the other way round. Pruning either dropped a bound that
-- excludes a row.
--
-- `optimize_redundant_comparisons` is pinned on every query because it is the setting under test and
-- every scenario pins both its enabled and its disabled answer; `clickhouse-test` does not randomize
-- it. `optimize_and_compare_chain` is pinned because `clickhouse-test` does randomize it: to 1 on the
-- result scenarios, so they run the configuration a user has, and to 0 on the `EXPLAIN QUERY TREE`
-- arms, where a derived conjunct wrapped in `indexHint` also matches the `ILIKE` pattern and would
-- move the node counts.

SET enable_analyzer = 1;

-- 1) A pair of `Array` bounds with no least supertype (`UInt64` against `Int64`).
--    The two single-condition counts are the ground truth: one bound matches the row, the other does
--    not, so the conjunction matches nothing.
DROP TABLE IF EXISTS t_element_wise_array;
CREATE TABLE t_element_wise_array (id UInt32, a Array(Tuple(Float64, UInt64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_array VALUES (1, [(nan, toUInt64(9))]);
SELECT count() FROM t_element_wise_array WHERE a >= [(1., toUInt64(1))] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_array WHERE a >= [(0., toInt64(0))] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_array WHERE a >= [(1., toUInt64(1))] AND a >= [(0., toInt64(0))] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_array WHERE a >= [(1., toUInt64(1))] AND a >= [(0., toInt64(0))] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- 2) A pair of top-level `Tuple`s. `executeTuple` is reached before the equal-types shortcut, so the
--    top level is decomposed even here, where the two constants differ only one level down.
DROP TABLE IF EXISTS t_element_wise_tuple;
CREATE TABLE t_element_wise_tuple (id UInt32, tu Tuple(Array(Tuple(Float64, UInt64)))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_tuple VALUES (1, tuple([(nan, toUInt64(9))]));
SELECT count() FROM t_element_wise_tuple WHERE tu >= tuple([(1., toUInt64(1))]) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_tuple WHERE tu >= tuple([(0., toInt64(0))]) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_tuple WHERE tu >= tuple([(1., toUInt64(1))]) AND tu >= tuple([(0., toInt64(0))]) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_tuple WHERE tu >= tuple([(1., toUInt64(1))]) AND tu >= tuple([(0., toInt64(0))]) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- 3) A constant `String` against a `Tuple` column. `executeWithConstString` converts the literal to
--    the column's type and re-enters the dispatch, so the comparison is decomposed even though the
--    constant is declared a `String`.
DROP TABLE IF EXISTS t_element_wise_const_string;
CREATE TABLE t_element_wise_const_string (id UInt32, tu Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_const_string VALUES (1, (3., 3.));
SELECT count() FROM t_element_wise_const_string WHERE tu = '(3,3)' SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_const_string WHERE tu <= '(nan,9)' SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_const_string WHERE tu = '(3,3)' AND tu <= '(nan,9)' SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_const_string WHERE tu = '(3,3)' AND tu <= '(nan,9)' SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- The result arms above also pass if the optimization stops running altogether, so pin what still has
-- to be pruned. Both the enabled and the disabled count are pinned: a relative comparison also holds
-- if the predicates disappear entirely.
DROP TABLE IF EXISTS t_element_wise_live;
CREATE TABLE t_element_wise_live (id UInt32, a Array(Tuple(Float64, UInt64)), m Map(String, Float64), i UInt32, j Int32, tu Tuple(Int64, Int64), af Array(Float64), s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_live VALUES (1, [(1., toUInt64(2))], map('k', 3.), 3, 3, (3, 3), [1.], '(1,2)');

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

-- 10) Both operands constant. The pass takes the left operand as the constant whenever it is one, so a
--     container constant on the left leaves the `String` playing the expression, and the comparison has
--     to be classified from whichever side carries the string. An ASOF `JOIN ON` is what lets two
--     constant comparisons reach the pass, a plain `WHERE` folds the conjunction first; and
--     `enable_scalar_subquery_optimization = 0` is what keeps the multi-column scalar subquery a
--     constant rather than a `__getScalar` call. The single-condition counts are the ground truth
--     again: one operand is false, so the conjunction matches nothing.
SELECT count() FROM (SELECT 1 AS k, 1 AS t) AS l ASOF INNER JOIN (SELECT 1 AS k, 1 AS t) AS r ON l.k = r.k AND l.t >= r.t AND (SELECT nan, 9.) >= '(3,3)' SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, enable_scalar_subquery_optimization = 0;
SELECT count() FROM (SELECT 1 AS k, 1 AS t) AS l ASOF INNER JOIN (SELECT 1 AS k, 1 AS t) AS r ON l.k = r.k AND l.t >= r.t AND (SELECT 4., 4.) >= '(3,3)' SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, enable_scalar_subquery_optimization = 0;
SELECT count() FROM (SELECT 1 AS k, 1 AS t) AS l ASOF INNER JOIN (SELECT 1 AS k, 1 AS t) AS r ON l.k = r.k AND l.t >= r.t AND (SELECT nan, 9.) >= '(3,3)' AND (SELECT 4., 4.) >= '(3,3)' SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, enable_scalar_subquery_optimization = 0;
SELECT count() FROM (SELECT 1 AS k, 1 AS t) AS l ASOF INNER JOIN (SELECT 1 AS k, 1 AS t) AS r ON l.k = r.k AND l.t >= r.t AND (SELECT nan, 9.) >= '(3,3)' AND (SELECT 4., 4.) >= '(3,3)' SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, enable_scalar_subquery_optimization = 0;

--     Mirrored, with the `String` constant on the left, which was already classified correctly. Same
--     four answers whatever the classification, so this is what isolates the arm above to operand order.
SELECT count() FROM (SELECT 1 AS k, 1 AS t) AS l ASOF INNER JOIN (SELECT 1 AS k, 1 AS t) AS r ON l.k = r.k AND l.t >= r.t AND '(3,3)' <= (SELECT nan, 9.) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, enable_scalar_subquery_optimization = 0;
SELECT count() FROM (SELECT 1 AS k, 1 AS t) AS l ASOF INNER JOIN (SELECT 1 AS k, 1 AS t) AS r ON l.k = r.k AND l.t >= r.t AND '(3,3)' <= (SELECT 4., 4.) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, enable_scalar_subquery_optimization = 0;
SELECT count() FROM (SELECT 1 AS k, 1 AS t) AS l ASOF INNER JOIN (SELECT 1 AS k, 1 AS t) AS r ON l.k = r.k AND l.t >= r.t AND '(3,3)' <= (SELECT nan, 9.) AND '(3,3)' <= (SELECT 4., 4.) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, enable_scalar_subquery_optimization = 0;
SELECT count() FROM (SELECT 1 AS k, 1 AS t) AS l ASOF INNER JOIN (SELECT 1 AS k, 1 AS t) AS r ON l.k = r.k AND l.t >= r.t AND '(3,3)' <= (SELECT nan, 9.) AND '(3,3)' <= (SELECT 4., 4.) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, enable_scalar_subquery_optimization = 0;

-- 11) A `String` column against a container constant is the other shape whose expression side carries
--     the string. It has no common type at all, so all three spellings have to be rejected alike: the
--     fold must not turn a query the server cannot execute into an answer.
SELECT count() FROM t_element_wise_live WHERE s <= (1, 2) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM t_element_wise_live WHERE s = '(1,2)' AND s <= (1, 2) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM t_element_wise_live WHERE s = '(1,2)' AND s <= (1, 2) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1; -- { serverError NO_COMMON_TYPE }

-- 12) A `NULL` nested in a container constant. `Field` orders it by its type tag, before every value,
--     while `IColumn::compareAt` orders it after every value, so the bound the analysis keeps is not the
--     one execution treats as tighter. Comparing two such containers yields a plain `UInt8`, so the
--     nullable-result screen never sees this. The single-condition counts are the ground truth again.
DROP TABLE IF EXISTS t_element_wise_nested_null;
CREATE TABLE t_element_wise_nested_null (id UInt32, a Array(Nullable(UInt32)), alc Array(LowCardinality(Nullable(String))), m Map(String, Nullable(Float64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_nested_null VALUES (1, [NULL], [NULL], map('k', NULL));
SELECT count() FROM t_element_wise_nested_null WHERE a <= [NULL] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_nested_null WHERE a <= [1] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_nested_null WHERE a <= [NULL] AND a <= [1] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_nested_null WHERE a <= [NULL] AND a <= [1] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- 13) The same with the `Nullable` under a `LowCardinality`, which a screen on the type would have to
--     strip to see. A screen on the constant has no wrapper to strip.
SELECT count() FROM t_element_wise_nested_null WHERE alc <= [NULL] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_nested_null WHERE alc <= ['b'] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_nested_null WHERE alc <= [NULL] AND alc <= ['b'] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_nested_null WHERE alc <= [NULL] AND alc <= ['b'] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- 14) The same for a `Map`, which is never decomposed, so this arm holds independently of which shapes
--     the element-wise classifier covers.
SELECT count() FROM t_element_wise_nested_null WHERE m <= map('k', NULL) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_nested_null WHERE m <= map('k', 1.) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_nested_null WHERE m <= map('k', NULL) AND m <= map('k', 1.) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_nested_null WHERE m <= map('k', NULL) AND m <= map('k', 1.) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- 15) The folds that must survive: the same nullable element types, with constants that carry no `NULL`.
--     These are what distinguish screening the constant from screening the type, which would give them up.
DROP TABLE IF EXISTS t_element_wise_nullable_live;
CREATE TABLE t_element_wise_nullable_live (id UInt32, an Array(Nullable(UInt32)), mn Map(String, Nullable(Float64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_nullable_live VALUES (1, [5], map('k', 5.));
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_nullable_live WHERE an = [5] AND an >= [0] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_nullable_live WHERE an = [5] AND an >= [0] SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_nullable_live WHERE mn = map('k', 5.) AND mn >= map('k', 0.) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_nullable_live WHERE mn = map('k', 5.) AND mn >= map('k', 0.) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';

-- 16) The same divergence one container further out, in a `JSON` value. It is the fourth container that
--     nests `Field`s, and the only one held as a map rather than as a vector, so a walk written over the
--     other three does not reach it. `ColumnObject::compareAt` delegates each path's value, which puts a
--     typed `Nullable` path back on `ColumnNullable`.
DROP TABLE IF EXISTS t_element_wise_json;
CREATE TABLE t_element_wise_json (id UInt32, j JSON(a Nullable(UInt32))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_json VALUES (1, '{"a":null}');
SELECT count() FROM t_element_wise_json WHERE j <= '{"a":null}'::JSON(a Nullable(UInt32)) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_json WHERE j <= '{"a":1}'::JSON(a Nullable(UInt32)) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_json WHERE j <= '{"a":null}'::JSON(a Nullable(UInt32)) AND j <= '{"a":1}'::JSON(a Nullable(UInt32)) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;
SELECT count() FROM t_element_wise_json WHERE j <= '{"a":null}'::JSON(a Nullable(UInt32)) AND j <= '{"a":1}'::JSON(a Nullable(UInt32)) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1;

-- 17) The `JSON` fold that must survive, on the same type and the same table: only the constants differ
--     from the arm above, so this is the one a screen on the type rather than on the constant gives up.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_json WHERE j = '{"a":5}'::JSON(a Nullable(UInt32)) AND j >= '{"a":0}'::JSON(a Nullable(UInt32)) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_json WHERE j = '{"a":5}'::JSON(a Nullable(UInt32)) AND j >= '{"a":0}'::JSON(a Nullable(UInt32)) SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0) WHERE explain ILIKE '%function_name: greaterOrEquals,%';

-- 18) Such a comparison must also stay out of the `notEquals` merge into `NOT IN`, whose set membership
--     uses the same order it is not a point in. Three constants of one value at three declared types
--     stay three entries here, because the merge deduplicates by node structure, and three is the
--     threshold; the resulting `notIn` holds `nan` equal to `nan` where the element-wise `notEquals` it
--     replaces does not. The threshold is pinned rather than left at its default so that changing that
--     default cannot silently take the arm below it. There is no disabled-setting spelling because
--     disabling the setting routes every filter to the same merge, which is what master already does.
DROP TABLE IF EXISTS t_element_wise_not_in;
CREATE TABLE t_element_wise_not_in (id UInt32, tu Tuple(Float64, Float64), a Array(Tuple(Float64, UInt64)), f Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_element_wise_not_in VALUES (1, (nan, 9.), [(nan, toUInt64(9))], 5.);
SELECT count() FROM t_element_wise_not_in WHERE tu != tuple(toFloat32(nan), toUInt8(9)) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3;
SELECT count() FROM t_element_wise_not_in WHERE tu != tuple(toFloat32(nan), toUInt8(9)) AND tu != tuple(nan, toUInt16(9)) AND tu != tuple(nan, toUInt32(9)) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3;
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_not_in WHERE tu != tuple(toFloat32(nan), toUInt8(9)) AND tu != tuple(nan, toUInt16(9)) AND tu != tuple(nan, toUInt32(9)) SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0, optimize_min_inequality_conjunction_chain_length = 3) WHERE explain ILIKE '%function_name: notIn,%';

-- 19) The same for the no-supertype `Array` spelling.
SELECT count() FROM t_element_wise_not_in WHERE a != [(toFloat32(nan), toInt8(9))] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3;
SELECT count() FROM t_element_wise_not_in WHERE a != [(toFloat32(nan), toInt8(9))] AND a != [(nan, toInt16(9))] AND a != [(nan, toInt32(9))] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3;
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_not_in WHERE a != [(toFloat32(nan), toInt8(9))] AND a != [(nan, toInt16(9))] AND a != [(nan, toInt32(9))] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0, optimize_min_inequality_conjunction_chain_length = 3) WHERE explain ILIKE '%function_name: notIn,%';

-- 20) The merge that must survive: a bare `NaN` constant is held aside from the pruning analysis by an
--     older screen, which does not set this flag, so it still merges. What is excluded is the flagged
--     filter, not every opaque one. Two ordinary conjuncts make up the three entries; the second query
--     drops the `NaN` conjunct and leaves two, which shows the held aside one is carrying the merge.
SELECT count() = 1 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_not_in WHERE f != nan AND f != 1. AND f != 2. SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0, optimize_min_inequality_conjunction_chain_length = 3) WHERE explain ILIKE '%function_name: notIn,%';
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_not_in WHERE f != 1. AND f != 2. SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0, optimize_min_inequality_conjunction_chain_length = 3) WHERE explain ILIKE '%function_name: notIn,%';

-- 21) The merge also has to leave the rejection of arm 11 alone: a `String` against a container has no
--     common type, and `NOT IN` reports a different error for it than the comparison does.
SELECT count() FROM t_element_wise_live WHERE s != CAST((1, 2), 'Tuple(UInt8, UInt8)') AND s != CAST((1, 2), 'Tuple(UInt16, UInt16)') AND s != CAST((1, 2), 'Tuple(UInt32, UInt32)') SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, optimize_min_inequality_conjunction_chain_length = 3; -- { serverError NO_COMMON_TYPE }

-- 22) The nested-`NULL` constant of arm 12 reaches the merge too, and is held out of it by the same flag.
--     Here the two notions of equality happen to agree, so this exclusion is a conservative choice rather
--     than a correctness requirement: the arm pins the choice, not a result.
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_element_wise_nested_null WHERE a != [NULL] AND a != [1] AND a != [2] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 0, optimize_min_inequality_conjunction_chain_length = 3) WHERE explain ILIKE '%function_name: notIn,%';

DROP TABLE t_element_wise_array;
DROP TABLE t_element_wise_tuple;
DROP TABLE t_element_wise_const_string;
DROP TABLE t_element_wise_live;
DROP TABLE t_element_wise_nested_null;
DROP TABLE t_element_wise_nullable_live;
DROP TABLE t_element_wise_json;
DROP TABLE t_element_wise_not_in;
