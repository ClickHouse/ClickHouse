-- Prove that `dotProductTransposed` (and its alias `scalarProductTransposed`) read only the requested bit planes
-- `vec.1` .. `vec.p` instead of the whole `QBit` column when `optimize_qbit_distance_function_reads` is enabled.
-- This is the plan-level companion to `04403_dot_product_transposed`, which only checks the returned values and
-- would still pass if `dotProductTransposed` were dropped from `DistanceTransposedPartialReadsPass`.
-- Companion to `03375_l2_distance_transposed_partial_reads_pass`.
-- https://github.com/ClickHouse/ClickHouse/pull/108100

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;

CREATE TABLE qbit (id UInt32, vec QBit(BFloat16, 16)) ENGINE = Memory;

SET optimize_qbit_distance_function_reads = true;

SELECT '-- dotProductTransposed, optimization enabled: reads vec.1 .. vec.4';
EXPLAIN actions=1
WITH arrayMap(i -> i * 2, range(16)) AS reference_vec
SELECT id, dotProductTransposed(vec, reference_vec, 4) AS dist FROM qbit;

SELECT '-- scalarProductTransposed alias, optimization enabled: reads vec.1 .. vec.4';
EXPLAIN actions=1
WITH arrayMap(i -> i * 2, range(16)) AS reference_vec
SELECT id, scalarProductTransposed(vec, reference_vec, 4) AS dist FROM qbit;

SET optimize_qbit_distance_function_reads = false;

SELECT '-- dotProductTransposed, optimization disabled: reads the whole vec column';
EXPLAIN actions=1
WITH arrayMap(i -> i * 2, range(16)) AS reference_vec
SELECT id, dotProductTransposed(vec, reference_vec, 4) AS dist FROM qbit;

SELECT '-- scalarProductTransposed alias, optimization disabled: reads the whole vec column';
EXPLAIN actions=1
WITH arrayMap(i -> i * 2, range(16)) AS reference_vec
SELECT id, scalarProductTransposed(vec, reference_vec, 4) AS dist FROM qbit;

DROP TABLE qbit;
