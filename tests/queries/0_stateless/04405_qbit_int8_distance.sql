-- Tests for the transposed distance functions over Int8 QBit

DROP TABLE IF EXISTS qbit;
CREATE TABLE qbit (id UInt32, vec QBit(Int8, 8)) ENGINE = Memory;
INSERT INTO qbit VALUES (1, [0, 1, 2, 3, 4, 5, 6, 7]);
INSERT INTO qbit VALUES (2, [1, 2, 3, 4, 5, 6, 7, 8]);
INSERT INTO qbit VALUES (3, [-8, -7, -6, -5, -4, -3, -2, -1]);

SELECT '========== optimize_qbit_distance_function_reads = 0 ==========';
SET optimize_qbit_distance_function_reads = 0;

SELECT 'L2DistanceTransposed, full precision (8 bits)';
WITH [0, 1, 2, 3, 4, 5, 6, 7] AS reference_vec
SELECT id, round(L2DistanceTransposed(vec, reference_vec, 8), 3) AS dist FROM qbit ORDER BY id;

SELECT 'cosineDistanceTransposed, full precision (8 bits)';
WITH [0, 1, 2, 3, 4, 5, 6, 7] AS reference_vec
SELECT id, round(cosineDistanceTransposed(vec, reference_vec, 8), 2) AS dist FROM qbit ORDER BY id;

SELECT 'L2DistanceTransposed, reduced precision (4 bits)';
WITH [0, 1, 2, 3, 4, 5, 6, 7] AS reference_vec
SELECT id, round(L2DistanceTransposed(vec, reference_vec, 4), 3) AS dist FROM qbit ORDER BY id;

SELECT '========== optimize_qbit_distance_function_reads = 1 ==========';
SET optimize_qbit_distance_function_reads = 1;

SELECT 'L2DistanceTransposed, full precision (8 bits)';
WITH [0, 1, 2, 3, 4, 5, 6, 7] AS reference_vec
SELECT id, round(L2DistanceTransposed(vec, reference_vec, 8), 3) AS dist FROM qbit ORDER BY id;

SELECT 'cosineDistanceTransposed, full precision (8 bits)';
WITH [0, 1, 2, 3, 4, 5, 6, 7] AS reference_vec
SELECT id, round(cosineDistanceTransposed(vec, reference_vec, 8), 2) AS dist FROM qbit ORDER BY id;

SELECT 'L2DistanceTransposed, reduced precision (4 bits)';
WITH [0, 1, 2, 3, 4, 5, 6, 7] AS reference_vec
SELECT id, round(L2DistanceTransposed(vec, reference_vec, 4), 3) AS dist FROM qbit ORDER BY id;

DROP TABLE qbit;

-- Large dimensions must not overflow the SimSIMD i8 int32 accumulator (we fall back to the
-- Float64 scalar path). Worst case: all 127 vs all -128.
SELECT '========== large dimension (no int32 overflow) ==========';
-- L2: per-element (127 - (-128))^2 = 65025; 65025 * 33026 > INT32_MAX. Result = sqrt(65025 * 33026).
CREATE TABLE qbit_l2 (vec QBit(Int8, 33026)) ENGINE = Memory;
INSERT INTO qbit_l2 SELECT CAST(arrayMap(x -> toInt8(127), range(33026)) AS QBit(Int8, 33026));
WITH arrayMap(x -> toInt8(-128), range(33026)) AS reference_vec
SELECT round(L2DistanceTransposed(vec, reference_vec, 8), 3) AS dist FROM qbit_l2;
DROP TABLE qbit_l2;
-- cosine: sum(a^2) = 128^2 * 131072 > INT32_MAX. cos(127s, -128s) = -1, so distance = 2.
CREATE TABLE qbit_cos (vec QBit(Int8, 131072)) ENGINE = Memory;
INSERT INTO qbit_cos SELECT CAST(arrayMap(x -> toInt8(127), range(131072)) AS QBit(Int8, 131072));
WITH arrayMap(x -> toInt8(-128), range(131072)) AS reference_vec
SELECT round(cosineDistanceTransposed(vec, reference_vec, 8), 3) AS dist FROM qbit_cos;
DROP TABLE qbit_cos;

-- Precision must be within the element bit-width [1, 8]
SELECT '========== precision validation ==========';
CREATE TABLE qbit (id UInt32, vec QBit(Int8, 4)) ENGINE = Memory;
INSERT INTO qbit VALUES (1, [1, 2, 3, 4]);
WITH [1, 2, 3, 4] AS reference_vec SELECT L2DistanceTransposed(vec, reference_vec, 9) FROM qbit; -- { serverError BAD_ARGUMENTS }
WITH [1, 2, 3, 4] AS reference_vec SELECT L2DistanceTransposed(vec, reference_vec, 0) FROM qbit; -- { serverError BAD_ARGUMENTS }
DROP TABLE qbit;
