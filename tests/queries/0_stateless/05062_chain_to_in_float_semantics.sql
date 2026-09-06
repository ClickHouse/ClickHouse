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

DROP TABLE t_chain_to_in_float;
DROP TABLE t_chain_to_in_int;
