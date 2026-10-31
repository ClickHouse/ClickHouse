-- Tags: no-old-analyzer
-- The rewrite being fixed here lives in the analyzer, and `EXPLAIN QUERY TREE` is analyzer-only.

-- The analyzer rewrites a comparison chain into `IN`/`NOT IN`, but `equals` compares floats
-- numerically (`-0.0 = 0.0` is true, `nan = nan` is false) while set membership is keyed on the raw
-- bits (`-0.0 IN (0.0)` is false, `nan IN (nan)` is true). With a floating-point column the
-- rewritten chain therefore disagreed with the comparisons it replaced, so such chains are left as-is.

DROP TABLE IF EXISTS t_chain_float;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_chain_float (f Float64, f32 Float32, bf BFloat16, arr Array(Float64), n Nullable(Float64), lc LowCardinality(Float64)) ENGINE = Memory;
INSERT INTO t_chain_float VALUES (-0.0, -0.0, -0.0, [-0.0], -0.0, -0.0), (0.0, 0.0, 0.0, [0.0], 0.0, 0.0), (nan, nan, nan, [nan], nan, nan), (1.0, 1.0, 1.0, [1.0], NULL, 1.0), (2.5, 2.5, 2.5, [2.5], 2.5, 2.5);

SELECT 'ground truth';
SELECT -0.0 = 0.0, -0.0 IN (0.0), nan = nan, nan IN (nan);

SELECT 'equals chain';
SELECT f, (f = 0.0 OR f = 1.0 OR f = 2.0) AS chain
FROM t_chain_float ORDER BY toString(f) SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT f, (f = nan OR f = 1.0 OR f = 2.0) AS chain
FROM t_chain_float ORDER BY toString(f) SETTINGS optimize_min_equality_disjunction_chain_length = 3;

SELECT 'not equals chain';
SELECT f, (f != 0.0 AND f != 1.0 AND f != 2.0) AS chain
FROM t_chain_float ORDER BY toString(f) SETTINGS optimize_min_inequality_conjunction_chain_length = 3;
SELECT f, (f != nan AND f != 1.0 AND f != 2.0) AS chain
FROM t_chain_float ORDER BY toString(f) SETTINGS optimize_min_inequality_conjunction_chain_length = 3;

SELECT 'in where';
SELECT count() FROM t_chain_float WHERE f = 0.0 OR f = 1.0 OR f = 2.0 SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM t_chain_float WHERE f != 0.0 AND f != 1.0 AND f != 2.0 SETTINGS optimize_min_inequality_conjunction_chain_length = 3;
SELECT count() FROM t_chain_float WHERE f = nan OR f = 1.0 OR f = 2.0 SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM t_chain_float WHERE f != nan AND f != 1.0 AND f != 2.0 SETTINGS optimize_min_inequality_conjunction_chain_length = 3;

SELECT 'other float carriers';
SELECT count() FROM t_chain_float WHERE f32 = 0.0 OR f32 = 1.0 OR f32 = 2.0 SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM t_chain_float WHERE bf = 0.0 OR bf = 1.0 OR bf = 2.0 SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM t_chain_float WHERE arr = [0.0] OR arr = [1.0] OR arr = [2.0] SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM t_chain_float WHERE n = 0.0 OR n = 1.0 OR n = 2.0 SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM t_chain_float WHERE lc = 0.0 OR lc = 1.0 OR lc = 2.0 SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM t_chain_float WHERE arr != [0.0] AND arr != [1.0] AND arr != [2.0] SETTINGS optimize_min_inequality_conjunction_chain_length = 3;
SELECT count() FROM t_chain_float WHERE lc != 0.0 AND lc != 1.0 AND lc != 2.0 SETTINGS optimize_min_inequality_conjunction_chain_length = 3;

-- The chain over a floating-point expression stays a chain of comparisons.
SELECT 'not rewritten';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT f FROM t_chain_float WHERE f = 0.0 OR f = 1.0 OR f = 2.0 SETTINGS optimize_min_equality_disjunction_chain_length = 3) WHERE explain LIKE '%function_name: in%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT f FROM t_chain_float WHERE f != 0.0 AND f != 1.0 AND f != 2.0 SETTINGS optimize_min_inequality_conjunction_chain_length = 3) WHERE explain LIKE '%function_name: notIn%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT f FROM t_chain_float WHERE arr = [0.0] OR arr = [1.0] OR arr = [2.0] SETTINGS optimize_min_equality_disjunction_chain_length = 3) WHERE explain LIKE '%function_name: in%';

-- An integer column compared with float literals is still rewritten: the set converts the literals
-- to the column's type, where no signed zero or NaN exists.
SELECT 'still rewritten';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT number FROM numbers(3) WHERE number = 0.0 OR number = 1.0 OR number = 2.0 SETTINGS optimize_min_equality_disjunction_chain_length = 3) WHERE explain LIKE '%function_name: in%';

DROP TABLE t_chain_float;
