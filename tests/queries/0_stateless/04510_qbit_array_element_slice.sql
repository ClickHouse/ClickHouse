-- Tests for arrayElement and arraySlice on QBit: element access reads only the bit planes of the stride group
-- containing the element, and slices aligned to stride-group boundaries reuse the stored bit-plane streams.

SELECT 'arrayElement on QBit(Float32, 8)';
DROP TABLE IF EXISTS qbit;
CREATE TABLE qbit (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO qbit VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [-1, -2, -3, -4, -5, -6, -7, -8]);
SELECT id, vec[1], vec[8], vec[-1], arrayElement(vec, 3) FROM qbit ORDER BY id;
SELECT DISTINCT toTypeName(vec[1]) FROM qbit;

SELECT 'Out-of-range index gives the default value, or NULL for arrayElementOrNull';
SELECT vec[9], vec[-9], arrayElementOrNull(vec, 9), arrayElementOrNull(vec, 1) FROM qbit ORDER BY id;

SELECT 'Non-constant index';
SELECT id, vec[id], vec[-toInt64(id)], vec[id * 100], arrayElement(vec, materialize(0)) FROM qbit ORDER BY id;

SELECT 'Constant index 0 is an error';
SELECT vec[0] FROM qbit; -- { serverError ZERO_ARRAY_OR_TUPLE_INDEX }

SELECT 'All element types';
SELECT (CAST([1.5, -2.5, 3.25, 100]::Array(Float64) AS QBit(Float64, 4)))[3] AS v, toTypeName(v);
SELECT (CAST([1.5, -2.5, 3.5, 100]::Array(BFloat16) AS QBit(BFloat16, 4)))[2] AS v, toTypeName(v);
SELECT (CAST([1, -2, 3, 100]::Array(Int8) AS QBit(Int8, 4)))[4] AS v, toTypeName(v);

SELECT 'arrayElement on strided QBit(Float32, 16, 8)';
DROP TABLE IF EXISTS qbit_stride;
CREATE TABLE qbit_stride (id UInt32, vec QBit(Float32, 16, 8)) ENGINE = Memory;
INSERT INTO qbit_stride VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
SELECT vec[1], vec[8], vec[9], vec[16], vec[-1] FROM qbit_stride;
DROP TABLE qbit_stride;

SELECT 'arraySlice on QBit(Float32, 8)';
SELECT arraySlice(vec, 2, 3) AS s, toTypeName(s) FROM qbit ORDER BY id;
SELECT arraySlice(vec, 1, 4) AS s, toTypeName(s) FROM qbit ORDER BY id;
SELECT arraySlice(vec, 5) AS s, toTypeName(s) FROM qbit ORDER BY id;
SELECT arraySlice(vec, -3), arraySlice(vec, 2, -2), arraySlice(vec, -6, 3) FROM qbit ORDER BY id;
SELECT arraySlice(vec, 5, 100) AS clamped, toTypeName(clamped) FROM qbit ORDER BY id;
SELECT arraySlice(vec, 1) AS whole, toTypeName(whole) FROM qbit ORDER BY id;

SELECT 'arraySlice errors';
SELECT arraySlice(vec, 9) FROM qbit; -- { serverError BAD_ARGUMENTS }
SELECT arraySlice(vec, 0, 3) FROM qbit; -- { serverError BAD_ARGUMENTS }
SELECT arraySlice(vec, 1, 0) FROM qbit; -- { serverError BAD_ARGUMENTS }
SELECT arraySlice(vec, 2, -7) FROM qbit; -- { serverError BAD_ARGUMENTS }
SELECT arraySlice(vec, id, 2) FROM qbit; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'arraySlice on strided QBit keeps the stride when aligned to stride groups';
DROP TABLE IF EXISTS qbit_stride32;
CREATE TABLE qbit_stride32 (id UInt32, vec QBit(Float32, 32, 8)) ENGINE = Memory;
INSERT INTO qbit_stride32 VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]);
SELECT arraySlice(vec, 9, 8) AS s, toTypeName(s) FROM qbit_stride32;
SELECT arraySlice(vec, 9, 16) AS s, toTypeName(s) FROM qbit_stride32;
SELECT arraySlice(vec, 1) AS s, toTypeName(s) FROM qbit_stride32;
SELECT arraySlice(vec, 2, 10) AS s, toTypeName(s) FROM qbit_stride32;
SELECT arraySlice(vec, 9, 12) AS s, toTypeName(s) FROM qbit_stride32;

SELECT 'Distance over a sliced strided QBit';
SELECT L2DistanceTransposed(arraySlice(vec, 9, 8), [9., 10., 11., 12., 13., 14., 15., 16.]::Array(Float32), 32) FROM qbit_stride32;
DROP TABLE qbit_stride32;

SELECT 'Slices of QBits with other element types and non-multiple-of-8 dimensions';
SELECT arraySlice(CAST([1, 2, 3, 4, 5]::Array(Float32) AS QBit(Float32, 5)), 2, 3) AS s, toTypeName(s);
SELECT arraySlice(CAST([1, -2, 3, -4, 5]::Array(Int8) AS QBit(Int8, 5)), 2, 3) AS s, toTypeName(s);
SELECT arraySlice(CAST([1.5, 2.5, 3.5, 4.5]::Array(Float64) AS QBit(Float64, 4)), 3, 2) AS s, toTypeName(s);
SELECT arraySlice(CAST([1.5, 2.5, 3.5, 4.5]::Array(BFloat16) AS QBit(BFloat16, 4)), 2, 2) AS s, toTypeName(s);

SELECT 'Nullable(QBit)';
DROP TABLE IF EXISTS qbit_null;
CREATE TABLE qbit_null (id UInt32, vec Nullable(QBit(Float32, 8))) ENGINE = Memory;
INSERT INTO qbit_null VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]::QBit(Float32, 8)), (2, NULL);
SELECT id, vec[2], arraySlice(vec, 3, 2) FROM qbit_null ORDER BY id;
SELECT DISTINCT toTypeName(vec[2]), toTypeName(arraySlice(vec, 3, 2)) FROM qbit_null;
DROP TABLE qbit_null;

SELECT 'MergeTree round trip';
DROP TABLE IF EXISTS qbit_mt;
CREATE TABLE qbit_mt (id UInt32, vec QBit(Float32, 16, 8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO qbit_mt VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), (2, [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]);
SELECT id, vec[9], arraySlice(vec, 9, 8) FROM qbit_mt ORDER BY id;
DROP TABLE qbit_mt;

DROP TABLE qbit;
