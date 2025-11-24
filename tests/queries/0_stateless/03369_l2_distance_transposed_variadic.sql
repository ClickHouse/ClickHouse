SET allow_experimental_qbit_type = 1;

SELECT 'Test L2DistanceTransposed: BFloat16';
DROP TABLE IF EXISTS qbit;
CREATE TABLE qbit (id UInt32, vec QBit(BFloat16, 3)) ENGINE = Memory;

INSERT INTO qbit VALUES (1, [0,1,2]);
INSERT INTO qbit VALUES (2, [1,2,3]);
INSERT INTO qbit VALUES (3, [2,3,4]);


-- Use rounding to avoid minor precision differences between different architectures
WITH [toBFloat16(0), 1, 2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, vec.5, vec.6, vec.7, vec.8, vec.9, vec.10, vec.11, vec.12, vec.13, vec.14, vec.15, vec.16, 3, reference_vec), 5) AS dist FROM qbit ORDER BY id;
WITH [toBFloat16(0), 1, 2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, vec.5, vec.6, vec.7, vec.8, 3, reference_vec), 5) AS dist FROM qbit ORDER BY id;
WITH [toBFloat16(0), 1, 2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, 3, reference_vec), 5) AS dist FROM qbit ORDER BY id;
DROP TABLE qbit;


SELECT 'Test L2DistanceTransposed: Float32';
CREATE TABLE qbit (id UInt32, vec QBit(Float32, 3)) ENGINE = Memory;

INSERT INTO qbit VALUES (1, [0,1,2]);
INSERT INTO qbit VALUES (2, [1,2,3]);
INSERT INTO qbit VALUES (3, [2,3,4]);

-- Use rounding to avoid minor precision differences when using non-vectorized implementations of distance function
WITH [toFloat32(0), 1, 2] AS reference_vec SELECT id, round(round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, vec.5, vec.6, vec.7, vec.8, vec.9, vec.10, vec.11, vec.12, vec.13, vec.14, vec.15, vec.16, vec.17, vec.18, vec.19, vec.20, vec.21, vec.22, vec.23, vec.24, vec.25, vec.26, vec.27, vec.28, vec.29, vec.30, vec.31, vec.32, 3, reference_vec), 5), 5) AS dist FROM qbit ORDER BY id;
WITH [toFloat32(0), 1, 2] AS reference_vec SELECT id, round(round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, vec.5, vec.6, vec.7, vec.8, vec.9, vec.10, vec.11, vec.12, vec.13, vec.14, vec.15, vec.16, 3, reference_vec), 5), 5) AS dist FROM qbit ORDER BY id;
WITH [toFloat32(0), 1, 2] AS reference_vec SELECT id, round(round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, vec.5, vec.6, vec.7, vec.8, 3, reference_vec), 5), 5) AS dist FROM qbit ORDER BY id;
DROP TABLE qbit;


SELECT 'Test L2DistanceTransposed: Float64';
CREATE TABLE qbit (id UInt32, vec QBit(Float64, 3)) ENGINE = Memory;

INSERT INTO qbit VALUES (1, [0,1,2]);
INSERT INTO qbit VALUES (2, [1,2,3]);
INSERT INTO qbit VALUES (3, [2,3,4]);

WITH [toFloat64(0), 1, 2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, vec.5, vec.6, vec.7, vec.8, vec.9, vec.10, vec.11, vec.12, vec.13, vec.14, vec.15, vec.16, vec.17, vec.18, vec.19, vec.20, vec.21, vec.22, vec.23, vec.24, vec.25, vec.26, vec.27, vec.28, vec.29, vec.30, vec.31, vec.32, vec.33, vec.34, vec.35, vec.36, vec.37, vec.38, vec.39, vec.40, vec.41, vec.42, vec.43, vec.44, vec.45, vec.46, vec.47, vec.48, vec.49, vec.50, vec.51, vec.52, vec.53, vec.54, vec.55, vec.56, vec.57, vec.58, vec.59, vec.60, vec.61, vec.62, vec.63, vec.64, 3, reference_vec), 5) AS dist FROM qbit ORDER BY id;
WITH [toFloat64(0), 1, 2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, vec.5, vec.6, vec.7, vec.8, vec.9, vec.10, vec.11, vec.12, vec.13, vec.14, vec.15, vec.16, vec.17, vec.18, vec.19, vec.20, vec.21, vec.22, vec.23, vec.24, vec.25, vec.26, vec.27, vec.28, vec.29, vec.30, vec.31, vec.32, 3, reference_vec), 5) AS dist FROM qbit ORDER BY id;
WITH [toFloat64(0), 1, 2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, vec.5, vec.6, vec.7, vec.8, vec.9, vec.10, vec.11, vec.12, vec.13, vec.14, vec.15, vec.16, 3, reference_vec), 5) AS dist FROM qbit ORDER BY id;
DROP TABLE qbit;


SELECT 'Difficult test';

CREATE TABLE qbit (id UInt32, vec QBit(Float32, 3)) ENGINE = Memory;

INSERT INTO qbit SELECT number + 1 AS id, arrayMap(i -> toFloat32(i + number), range(3)) AS vec FROM numbers(3);

WITH [toFloat32(0), 1, 2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, 3, reference_vec), 5) AS dist FROM qbit ORDER BY id;
WITH [toFloat32(0), 1, 2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, 3.1, reference_vec), 5) AS dist FROM qbit ORDER BY id; -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }
WITH [toFloat32(0), 1, 2] AS reference_vec SELECT id, round(L2DistanceTransposed(vec.1, vec.2, vec.3, vec.4, 0, reference_vec), 5) AS dist FROM qbit ORDER BY id; -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }
DROP TABLE qbit;

-- Check that constant FixedString columns are supported
SELECT round(L2DistanceTransposed(''::FixedString(1), 3, [1,2,3]::Array(Float32)), 5); -- useDefaultImplementationForConstants is triggered
SELECT round(L2DistanceTransposed(''::FixedString(1), 3, materialize([1,2,3])::Array(Float32)), 5);
