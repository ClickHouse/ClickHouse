-- Folding an equality chain into `IN` must not change the comparison semantics: `IN` matches by set
-- membership, which disagrees with `equals` on NaN (`nan = nan` is 0, `nan IN (nan)` is 1) and on the
-- signed zero (`-0.0 = 0.0` is 1, `-0.0 IN (0.0)` is 0).

DROP TABLE IF EXISTS t_chain_to_in_float;
CREATE TABLE t_chain_to_in_float (f Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_chain_to_in_float VALUES (nan), (1.), (2.), (5.), (-0.0);

SELECT count(), (SELECT count() FROM t_chain_to_in_float WHERE (f != nan) AND (f != 1.) AND (f != 2.) SETTINGS optimize_min_inequality_conjunction_chain_length = 100000)
FROM t_chain_to_in_float WHERE (f != nan) AND (f != 1.) AND (f != 2.);

SELECT count(), (SELECT count() FROM t_chain_to_in_float WHERE (f = nan) OR (f = 1.) OR (f = 2.) SETTINGS optimize_min_equality_disjunction_chain_length = 100000)
FROM t_chain_to_in_float WHERE (f = nan) OR (f = 1.) OR (f = 2.);

SELECT count(), (SELECT count() FROM t_chain_to_in_float WHERE (f = 0.0) OR (f = 1.) OR (f = 2.) SETTINGS optimize_min_equality_disjunction_chain_length = 100000)
FROM t_chain_to_in_float WHERE (f = 0.0) OR (f = 1.) OR (f = 2.);

SELECT count(), (SELECT count() FROM t_chain_to_in_float WHERE (f != 0.0) AND (f != 1.) AND (f != 2.) SETTINGS optimize_min_inequality_conjunction_chain_length = 100000)
FROM t_chain_to_in_float WHERE (f != 0.0) AND (f != 1.) AND (f != 2.);

-- An integer literal reaches the comparison as `+0.0`, so it has the same problem.
SELECT count(), (SELECT count() FROM t_chain_to_in_float WHERE (f = 0) OR (f = 1) OR (f = 2) SETTINGS optimize_min_equality_disjunction_chain_length = 100000)
FROM t_chain_to_in_float WHERE (f = 0) OR (f = 1) OR (f = 2);

SELECT count(), (SELECT count() FROM t_chain_to_in_float WHERE (f != 0) AND (f != 1) AND (f != 2) SETTINGS optimize_min_inequality_conjunction_chain_length = 100000)
FROM t_chain_to_in_float WHERE (f != 0) AND (f != 1) AND (f != 2);

-- Chains without a NaN or a zero keep the conversion, and so do chains on a non-floating-point column.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_to_in_float WHERE (f = 3.) OR (f = 1.) OR (f = 2.)) WHERE explain ILIKE '%function_name: in%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_to_in_float WHERE (f = 0.) OR (f = 1.) OR (f = 2.)) WHERE explain ILIKE '%function_name: in%';

DROP TABLE IF EXISTS t_chain_to_in_int;
CREATE TABLE t_chain_to_in_int (i Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_chain_to_in_int VALUES (0), (1), (2), (5);

SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_to_in_int WHERE (i = 0) OR (i = 1) OR (i = 2)) WHERE explain ILIKE '%function_name: in%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_to_in_int WHERE (i != 0) AND (i != 1) AND (i != 2)) WHERE explain ILIKE '%function_name: notIn%';

-- The same divergence is reachable through a compound carrier: `equals` on a `Tuple` or an `Array` is
-- evaluated element-wise, while `IN` hashes the raw bits of every element.

DROP TABLE IF EXISTS t_chain_to_in_float_tuple;
CREATE TABLE t_chain_to_in_float_tuple (t Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_chain_to_in_float_tuple VALUES ((nan, 1.)), ((1., 2.)), ((3., 4.)), ((5., 6.)), ((-0.0, 7.));

SELECT count(), (SELECT count() FROM t_chain_to_in_float_tuple WHERE (t = (nan, 1.)) OR (t = (1., 2.)) OR (t = (3., 4.)) SETTINGS optimize_min_equality_disjunction_chain_length = 100000)
FROM t_chain_to_in_float_tuple WHERE (t = (nan, 1.)) OR (t = (1., 2.)) OR (t = (3., 4.));

SELECT count(), (SELECT count() FROM t_chain_to_in_float_tuple WHERE (t = (0., 7.)) OR (t = (1., 2.)) OR (t = (3., 4.)) SETTINGS optimize_min_equality_disjunction_chain_length = 100000)
FROM t_chain_to_in_float_tuple WHERE (t = (0., 7.)) OR (t = (1., 2.)) OR (t = (3., 4.));

SELECT count(), (SELECT count() FROM t_chain_to_in_float_tuple WHERE (t != (nan, 1.)) AND (t != (1., 2.)) AND (t != (3., 4.)) SETTINGS optimize_min_inequality_conjunction_chain_length = 100000)
FROM t_chain_to_in_float_tuple WHERE (t != (nan, 1.)) AND (t != (1., 2.)) AND (t != (3., 4.));

-- The set-membership relation really is a different one here, so the chain has to stay a comparison.
SELECT count() FROM t_chain_to_in_float_tuple WHERE t IN ((nan, 1.), (1., 2.), (3., 4.));
SELECT count() FROM t_chain_to_in_float_tuple WHERE t IN ((0., 7.), (1., 2.), (3., 4.));

SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_to_in_float_tuple WHERE (t = (nan, 1.)) OR (t = (1., 2.)) OR (t = (3., 4.))) WHERE explain ILIKE '%function_name: in%' SETTINGS enable_analyzer = 1;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_to_in_float_tuple WHERE (t = (0., 7.)) OR (t = (1., 2.)) OR (t = (3., 4.))) WHERE explain ILIKE '%function_name: in%' SETTINGS enable_analyzer = 1;
-- A tuple chain without a NaN and without a zero keeps the conversion.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_to_in_float_tuple WHERE (t = (9., 7.)) OR (t = (1., 2.)) OR (t = (3., 4.))) WHERE explain ILIKE '%function_name: in%' SETTINGS enable_analyzer = 1;

DROP TABLE IF EXISTS t_chain_to_in_float_array;
CREATE TABLE t_chain_to_in_float_array (arr Array(Float64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_chain_to_in_float_array VALUES ([nan]), ([1.]), ([2.]), ([5.]), ([-0.0]);

SELECT count(), (SELECT count() FROM t_chain_to_in_float_array WHERE (arr = [0.]) OR (arr = [1.]) OR (arr = [2.]) SETTINGS optimize_min_equality_disjunction_chain_length = 100000)
FROM t_chain_to_in_float_array WHERE (arr = [0.]) OR (arr = [1.]) OR (arr = [2.]);

SELECT count() FROM t_chain_to_in_float_array WHERE arr IN ([0.], [1.], [2.]);

SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_to_in_float_array WHERE (arr = [0.]) OR (arr = [1.]) OR (arr = [2.])) WHERE explain ILIKE '%function_name: in%' SETTINGS enable_analyzer = 1;
-- An array chain without a NaN and without a zero keeps the conversion.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_to_in_float_array WHERE (arr = [9.]) OR (arr = [1.]) OR (arr = [2.])) WHERE explain ILIKE '%function_name: in%' SETTINGS enable_analyzer = 1;

-- A compound carrier without floating-point elements keeps the conversion as well.
DROP TABLE IF EXISTS t_chain_to_in_int_tuple;
CREATE TABLE t_chain_to_in_int_tuple (t Tuple(Int32, Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_chain_to_in_int_tuple VALUES ((0, 1)), ((1, 2)), ((3, 4));

SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_chain_to_in_int_tuple WHERE (t = (0, 1)) OR (t = (1, 2)) OR (t = (3, 4))) WHERE explain ILIKE '%function_name: in%' SETTINGS enable_analyzer = 1;

-- The legacy `enable_analyzer = 0` rewrite (`LogicalExpressionsOptimizer`) folds the same chain and needs
-- the same guard.

SELECT count(), (SELECT count() FROM t_chain_to_in_float WHERE (f = nan) OR (f = 1.) OR (f = 2.) SETTINGS enable_analyzer = 0, optimize_min_equality_disjunction_chain_length = 100000)
FROM t_chain_to_in_float WHERE (f = nan) OR (f = 1.) OR (f = 2.) SETTINGS enable_analyzer = 0;

SELECT count(), (SELECT count() FROM t_chain_to_in_float WHERE (f = 0.) OR (f = 1.) OR (f = 2.) SETTINGS enable_analyzer = 0, optimize_min_equality_disjunction_chain_length = 100000)
FROM t_chain_to_in_float WHERE (f = 0.) OR (f = 1.) OR (f = 2.) SETTINGS enable_analyzer = 0;

SELECT count(), (SELECT count() FROM t_chain_to_in_float WHERE (f = 0) OR (f = 1) OR (f = 2) SETTINGS enable_analyzer = 0, optimize_min_equality_disjunction_chain_length = 100000)
FROM t_chain_to_in_float WHERE (f = 0) OR (f = 1) OR (f = 2) SETTINGS enable_analyzer = 0;

SELECT count() FROM (EXPLAIN SYNTAX SELECT count() FROM t_chain_to_in_float WHERE (f = nan) OR (f = 1.) OR (f = 2.) SETTINGS enable_analyzer = 0) WHERE explain ILIKE '%in(%' SETTINGS enable_analyzer = 0;
-- A chain without a NaN and without a zero, and a chain on a non-floating-point column, are still folded.
SELECT count() FROM (EXPLAIN SYNTAX SELECT count() FROM t_chain_to_in_float WHERE (f = 3.) OR (f = 1.) OR (f = 2.) SETTINGS enable_analyzer = 0) WHERE explain ILIKE '%in(%' SETTINGS enable_analyzer = 0;
SELECT count() FROM (EXPLAIN SYNTAX SELECT count() FROM t_chain_to_in_int WHERE (i = 0) OR (i = 1) OR (i = 2) SETTINGS enable_analyzer = 0) WHERE explain ILIKE '%in(%' SETTINGS enable_analyzer = 0;

DROP TABLE t_chain_to_in_float_tuple;
DROP TABLE t_chain_to_in_float_array;
DROP TABLE t_chain_to_in_int_tuple;

DROP TABLE t_chain_to_in_float;
DROP TABLE t_chain_to_in_int;
