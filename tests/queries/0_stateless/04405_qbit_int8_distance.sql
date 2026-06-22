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

-- Precision must be within the element bit-width [1, 8]
SELECT '========== precision validation ==========';
CREATE TABLE qbit (id UInt32, vec QBit(Int8, 4)) ENGINE = Memory;
INSERT INTO qbit VALUES (1, [1, 2, 3, 4]);
WITH [1, 2, 3, 4] AS reference_vec SELECT L2DistanceTransposed(vec, reference_vec, 9) FROM qbit; -- { serverError BAD_ARGUMENTS }
WITH [1, 2, 3, 4] AS reference_vec SELECT L2DistanceTransposed(vec, reference_vec, 0) FROM qbit; -- { serverError BAD_ARGUMENTS }
DROP TABLE qbit;
