SET enable_analyzer = 1;

-- Check if [L2/cosine/...]DistanceTransposed(vec, reference_vec, N) partial reads are achieved with optimize_qbit_distance_function_reads

CREATE TABLE qbit (id UInt32, vec QBit(BFloat16, 16)) ENGINE = Memory;

SET optimize_qbit_distance_function_reads = true;

EXPLAIN actions=1
WITH arrayMap(i -> i * 2, range(16)) AS reference_vec
SELECT id, L2DistanceTransposed(vec, reference_vec, 4) AS dist FROM qbit;

EXPLAIN actions=1
WITH arrayMap(i -> i * 2, range(16)) AS reference_vec
SELECT id, cosineDistanceTransposed(vec, reference_vec, 4) AS dist FROM qbit;


SET optimize_qbit_distance_function_reads = false;

EXPLAIN actions=1
WITH arrayMap(i -> i * 2, range(16)) AS reference_vec
SELECT id, L2DistanceTransposed(vec, reference_vec, 4) AS dist FROM qbit;

EXPLAIN actions=1
WITH arrayMap(i -> i * 2, range(16)) AS reference_vec
SELECT id, cosineDistanceTransposed(vec, reference_vec, 4) AS dist FROM qbit;

DROP TABLE qbit;



-- https://github.com/ClickHouse/ClickHouse/issues/88362

CREATE TABLE qbit (id UInt32, vec QBit(BFloat16, 1)) ENGINE = Memory;
INSERT INTO qbit VALUES (1, [toBFloat16(1)]);

WITH [2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec, reference_vec, toNullable(1)), 1) AS dist FROM qbit;
WITH [2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec, reference_vec, toLowCardinality(toNullable(1))), 1) AS dist FROM qbit;

DROP TABLE qbit;