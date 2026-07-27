-- Tests CAST of a strided QBit back to an Array. The reverse cast must untranspose each stride group
-- independently (mirroring the forward Array -> QBit conversion and the serialization path), otherwise
-- the trailing stride groups are dropped and the reconstruction reads past the per-group FixedString data.

SELECT 'Strided QBit -> Array reconstructs the full vector (all element types)';
SELECT CAST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(Float64, 16, 8) AS Array(Float64));
SELECT CAST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(Float32, 16, 8) AS Array(Float32));
SELECT CAST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(BFloat16, 16, 8) AS Array(BFloat16));
SELECT CAST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(Int8, 16, 8) AS Array(Int8));

SELECT 'The strided reconstruction is identical to the non-strided one for the same vector';
SELECT
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(Float32, 16, 8)::Array(Float32)
    = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(Float32, 16)::Array(Float32);
SELECT
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(Float64, 16, 8)::Array(Float64)
    = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(Float64, 16)::Array(Float64);

SELECT 'Round-trip Array -> QBit(strided) -> Array is lossless for several stride sizes';
SELECT [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]::QBit(Float64, 8, 8)::Array(Float64) = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]::Array(Float64);
SELECT [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6]::QBit(Float32, 16, 8)::Array(Float32) = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6]::Array(Float32);
SELECT range(32)::Array(Float64)::QBit(Float64, 32, 16)::Array(Float64) = range(32)::Array(Float64);
SELECT range(32)::Array(Float32)::QBit(Float32, 32, 8)::Array(Float32) = range(32)::Array(Float32);

SELECT 'Many stride groups (QBit(Float32, 32, 8) has four groups of eight dimensions)';
SELECT range(32)::Array(Float32)::QBit(Float32, 32, 8)::Array(Float32);

SELECT 'Int8 strided round trip, including negative and boundary values, is lossless';
SELECT [-128, -1, 0, 1, 2, 3, 4, 127]::QBit(Int8, 8, 8)::Array(Int8);
SELECT [-128, -1, 0, 1, 2, 3, 4, 127, 100, -100, 50, -50, 42, -42, 7, -7]::QBit(Int8, 16, 8)::Array(Int8) = [-128, -1, 0, 1, 2, 3, 4, 127, 100, -100, 50, -50, 42, -42, 7, -7]::Array(Int8);

SELECT 'Element type conversion during cast (QBit(Float32, 16, 8) -> Array(Float64))';
SELECT [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(Float32, 16, 8)::Array(Float64);

SELECT 'From a table with multiple rows, including a default (all-zero) row';
DROP TABLE IF EXISTS qbit_stride_to_array_test;
CREATE TABLE qbit_stride_to_array_test (id UInt32, vec QBit(Float32, 16, 8)) ENGINE = Memory;
INSERT INTO qbit_stride_to_array_test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), (2, [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
INSERT INTO qbit_stride_to_array_test (id) VALUES (3);
SELECT id, CAST(vec AS Array(Float32)) FROM qbit_stride_to_array_test ORDER BY id;
DROP TABLE qbit_stride_to_array_test;

SELECT 'Non-constant (materialized) strided QBit column';
SELECT materialize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(Float32, 16, 8))::Array(Float32);

SELECT 'Nullable strided QBit source: non-NULL rows reconstruct correctly';
DROP TABLE IF EXISTS qbit_stride_nullable_to_array_test;
CREATE TABLE qbit_stride_nullable_to_array_test (id UInt32, vec Nullable(QBit(Float32, 16, 8))) ENGINE = Memory;
INSERT INTO qbit_stride_nullable_to_array_test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), (2, [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
SELECT id, CAST(vec AS Array(Float32)) FROM qbit_stride_nullable_to_array_test ORDER BY id;
DROP TABLE qbit_stride_nullable_to_array_test;

SELECT 'Nullable strided QBit source: a NULL value cannot be cast to a non-Nullable Array';
SELECT CAST(materialize(CAST(NULL AS Nullable(QBit(Float32, 16, 8)))) AS Array(Float32)); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
