-- Matryoshka-style partial-dimension search on a strided QBit via the optional 4th `dims` argument.
-- A strided search over the first D dims must equal a non-strided search over a QBit holding only those D dims.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS qbit_strided;
DROP TABLE IF EXISTS qbit_plain;
CREATE TABLE qbit_strided (id UInt32, vec QBit(Float32, 16, 8)) ENGINE = Memory;
CREATE TABLE qbit_plain (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO qbit_strided VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), (2, [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
-- qbit_plain holds only the first 8 dimensions of each vector.
INSERT INTO qbit_plain VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [16, 15, 14, 13, 12, 11, 10, 9]);

SELECT 'L2: strided (first 8 dims) vs non-strided baseline, optimization on';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(L2DistanceTransposed(vec, ref, 32, 8), 4) FROM qbit_strided ORDER BY id;
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(L2DistanceTransposed(vec, ref, 32), 4) FROM qbit_plain ORDER BY id;

SELECT 'cosine: strided (first 8 dims) vs non-strided baseline, optimization on';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(cosineDistanceTransposed(vec, ref, 32, 8), 4) FROM qbit_strided ORDER BY id;
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(cosineDistanceTransposed(vec, ref, 32), 4) FROM qbit_plain ORDER BY id;

SELECT 'Same with the partial-reads optimization disabled (user-form fallback)';
SET optimize_qbit_distance_function_reads = 0;
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(L2DistanceTransposed(vec, ref, 32, 8), 4) FROM qbit_strided ORDER BY id;
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(cosineDistanceTransposed(vec, ref, 32, 8), 4) FROM qbit_strided ORDER BY id;
SET optimize_qbit_distance_function_reads = 1;

SELECT 'Full-dimension strided search (3-arg form reads all stride groups)';
WITH arrayMap(i -> toFloat32(i), range(16)) AS ref SELECT id, round(L2DistanceTransposed(vec, ref, 32), 4) FROM qbit_strided ORDER BY id;

SELECT 'Reduced precision on first 8 dims';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(L2DistanceTransposed(vec, ref, 8, 8), 4) FROM qbit_strided ORDER BY id;

-- A nullable precision or a nullable dims argument must propagate to a Nullable result, even though the
-- DistanceTransposedPartialReadsPass rewrites the call and removes those arguments (regression: it used to throw a logical error).
SELECT 'Nullable precision / dims propagate to a Nullable result';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT DISTINCT toTypeName(L2DistanceTransposed(vec, ref, toNullable(32), 8)) FROM qbit_strided;
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT DISTINCT toTypeName(L2DistanceTransposed(vec, ref, 32, toNullable(8))) FROM qbit_strided;
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(L2DistanceTransposed(vec, ref, 32, toNullable(8)), 4) FROM qbit_strided ORDER BY id;

SELECT 'Validation errors';
-- dims must be a multiple of the stride (8)
WITH [toFloat32(0), 1, 2, 3] AS ref SELECT L2DistanceTransposed(vec, ref, 32, 4) FROM qbit_strided; -- { serverError BAD_ARGUMENTS }
-- dims must not exceed the dimension (16)
WITH arrayMap(i -> toFloat32(i), range(24)) AS ref SELECT L2DistanceTransposed(vec, ref, 32, 24) FROM qbit_strided; -- { serverError BAD_ARGUMENTS }
-- the reference vector length must equal dims
WITH arrayMap(i -> toFloat32(i), range(16)) AS ref SELECT L2DistanceTransposed(vec, ref, 32, 8) FROM qbit_strided; -- { serverError BAD_ARGUMENTS }

DROP TABLE qbit_strided;
DROP TABLE qbit_plain;
