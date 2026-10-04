-- Tags: no-old-analyzer
-- The rewrite being fixed here lives in the analyzer, and `EXPLAIN QUERY TREE` is analyzer-only.

-- A comparison chain over a floating-point expression must not be rewritten into `IN`/`NOT IN`:
-- `equals` compares floats numerically (`-0.0 = 0.0` is true, `nan = nan` is false) while set
-- membership is keyed on the raw bits. A `Variant` with a floating-point alternative has the same
-- problem, since `equals` on a `Variant` compares the alternatives it holds.

SET enable_analyzer = 1;
SET allow_suspicious_variant_types = 1;
SET use_variant_default_implementation_for_comparisons = 1;
SET optimize_min_equality_disjunction_chain_length = 3;
SET optimize_min_inequality_conjunction_chain_length = 3;

DROP TABLE IF EXISTS t_chain_variant;
CREATE TABLE t_chain_variant (id UInt8, v Variant(Float64, UInt8), x UInt8) ENGINE = Memory;
INSERT INTO t_chain_variant VALUES (1, -0.0::Float64, 1), (2, 0.0::Float64, 1), (3, nan::Float64, 1), (4, 1.0::Float64, 1), (5, 2.5::Float64, 1), (6, 7::UInt8, 1);

SELECT 'equals chain';
SELECT id, (v = 0.0 OR v = 1.0 OR v = 2.0) FROM t_chain_variant ORDER BY id;
SELECT id, (v = nan OR v = 1.0 OR v = 2.0) FROM t_chain_variant ORDER BY id;

SELECT 'not equals chain';
SELECT id FROM t_chain_variant WHERE v != 0.0 AND v != 1.0 AND v != 2.0 AND x = 1 ORDER BY id;

SELECT 'not rewritten';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (v = 0.0) OR (v = 1.0) OR (v = 2.0) FROM t_chain_variant) WHERE explain LIKE '%function_name: in%';

-- A `Variant` without a floating-point alternative is still rewritten.
SELECT 'still rewritten';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT (w = 1) OR (w = 2) OR (w = 3) FROM (SELECT 1::UInt64::Variant(UInt64, String) AS w)) WHERE explain LIKE '%function_name: in%';

DROP TABLE t_chain_variant;
