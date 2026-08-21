-- Matryoshka-style partial-dimension search with `dotProductTransposed` on a strided QBit via the optional 4th `dims` argument.
-- A strided dot product over the first D dims must equal a non-strided dot product over a QBit holding only those D dims.
-- Companion to 04491_qbit_stride_distance (L2 / cosine) and 04489_dot_product_transposed_partial_reads_pass.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS qbit_strided;
DROP TABLE IF EXISTS qbit_plain;
CREATE TABLE qbit_strided (id UInt32, vec QBit(Float32, 16, 8)) ENGINE = Memory;
CREATE TABLE qbit_plain (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO qbit_strided VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), (2, [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
-- qbit_plain holds only the first 8 dimensions of each vector.
INSERT INTO qbit_plain VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [16, 15, 14, 13, 12, 11, 10, 9]);

SELECT 'dotProduct: strided (first 8 dims) vs non-strided baseline, optimization on';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(dotProductTransposed(vec, ref, 32, 8), 4) FROM qbit_strided ORDER BY id;
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(dotProductTransposed(vec, ref, 32), 4) FROM qbit_plain ORDER BY id;

SELECT 'scalarProductTransposed alias: strided (first 8 dims)';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(scalarProductTransposed(vec, ref, 32, 8), 4) FROM qbit_strided ORDER BY id;

SELECT 'Same with the partial-reads optimization disabled (user-form fallback)';
SET optimize_qbit_distance_function_reads = 0;
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(dotProductTransposed(vec, ref, 32, 8), 4) FROM qbit_strided ORDER BY id;
SET optimize_qbit_distance_function_reads = 1;

SELECT 'Full-dimension strided search (3-arg form reads all stride groups)';
WITH arrayMap(i -> toFloat32(i), range(16)) AS ref SELECT id, round(dotProductTransposed(vec, ref, 32), 4) FROM qbit_strided ORDER BY id;

SELECT 'Reduced precision on first 8 dims';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT id, round(dotProductTransposed(vec, ref, 8, 8), 4) FROM qbit_strided ORDER BY id;

-- A nullable precision or a nullable dims argument must propagate to a Nullable result, even though the
-- DistanceTransposedPartialReadsPass rewrites the call and removes those arguments.
SELECT 'Nullable precision / dims propagate to a Nullable result';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT DISTINCT toTypeName(dotProductTransposed(vec, ref, toNullable(32), 8)) FROM qbit_strided;
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref SELECT DISTINCT toTypeName(dotProductTransposed(vec, ref, 32, toNullable(8))) FROM qbit_strided;

-- Prove the partial-read I/O contract: the DistanceTransposedPartialReadsPass must rewrite a reduced-dimension search to read ONLY the
-- bit-plane streams of the covered stride groups, not the whole QBit column. We count the distinct `vec.N` subcolumns referenced in the
-- query plan: a full read of the public QBit (the fallback) references none (it reads `vec` whole), so a non-zero, reduced count proves
-- the optimization fired and read fewer streams. For QBit(BFloat16, 4096, 1024): element_size = 16 planes, 4 stride groups => 64 streams.
SELECT 'Partial-read I/O contract: distinct vec.N streams referenced in the plan';
DROP TABLE IF EXISTS qbit_io;
CREATE TABLE qbit_io (id UInt32, vec QBit(BFloat16, 4096, 1024)) ENGINE = MergeTree ORDER BY id;
INSERT INTO qbit_io SELECT number, arrayMap(i -> toFloat32(i), range(4096)) FROM numbers(2);
-- 8 bits over the first 2048 of 4096 dims (stride 1024) => 8 planes x 2 groups = 16 streams.
SELECT 'p=8 dims=2048 (expect 16)', arrayUniq(extractAll(arrayStringConcat((SELECT groupArray(explain) FROM (EXPLAIN actions = 1 WITH arrayMap(i -> toBFloat16(i), range(2048)) AS ref SELECT dotProductTransposed(vec, ref, 8, 2048) FROM qbit_io)), ' '), 'vec\\.[0-9]+'));
-- full precision over all dims => 16 planes x 4 groups = 64 streams.
SELECT 'p=16 dims=4096 (expect 64)', arrayUniq(extractAll(arrayStringConcat((SELECT groupArray(explain) FROM (EXPLAIN actions = 1 WITH arrayMap(i -> toBFloat16(i), range(4096)) AS ref SELECT dotProductTransposed(vec, ref, 16, 4096) FROM qbit_io)), ' '), 'vec\\.[0-9]+'));
-- With the optimization disabled, the whole `vec` column is read, so no per-stream subcolumns appear in the plan.
SET optimize_qbit_distance_function_reads = 0;
SELECT 'optimization off (expect 0)', arrayUniq(extractAll(arrayStringConcat((SELECT groupArray(explain) FROM (EXPLAIN actions = 1 WITH arrayMap(i -> toBFloat16(i), range(2048)) AS ref SELECT dotProductTransposed(vec, ref, 8, 2048) FROM qbit_io)), ' '), 'vec\\.[0-9]+'));
SET optimize_qbit_distance_function_reads = 1;
DROP TABLE qbit_io;

SELECT 'Validation errors';
-- dims must be a multiple of the stride (8)
WITH [toFloat32(0), 1, 2, 3] AS ref SELECT dotProductTransposed(vec, ref, 32, 4) FROM qbit_strided; -- { serverError BAD_ARGUMENTS }
-- dims must not exceed the dimension (16)
WITH arrayMap(i -> toFloat32(i), range(24)) AS ref SELECT dotProductTransposed(vec, ref, 32, 24) FROM qbit_strided; -- { serverError BAD_ARGUMENTS }
-- the reference vector length must equal dims
WITH arrayMap(i -> toFloat32(i), range(16)) AS ref SELECT dotProductTransposed(vec, ref, 32, 8) FROM qbit_strided; -- { serverError BAD_ARGUMENTS }

DROP TABLE qbit_strided;
DROP TABLE qbit_plain;
