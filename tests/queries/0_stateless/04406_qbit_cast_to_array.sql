-- Tests CAST of a QBit back to an Array, reconstructing the original vector from the
-- bit-transposed representation. This is the inverse of the Array -> QBit conversion.

SELECT 'Basic QBit -> Array, same element type';
SELECT CAST([1, 2, 3, 4]::QBit(Float64, 4) AS Array(Float64));
SELECT CAST([1, 2, 3, 4]::QBit(Float32, 4) AS Array(Float32));
SELECT CAST([1, 2, 3, 4]::QBit(BFloat16, 4) AS Array(BFloat16));

SELECT 'Operator-style cast';
SELECT [10, 20, 30]::QBit(Float64, 3)::Array(Float64);

SELECT 'Round-trip Array -> QBit -> Array is lossless for Float32/Float64';
SELECT [0.1, 0.2, 0.3, 0.4, 0.5]::QBit(Float64, 5)::Array(Float64);
SELECT [0.1, 0.2, 0.3, 0.4, 0.5]::QBit(Float32, 5)::Array(Float32);

SELECT 'Element type conversion during cast (QBit(Float32) -> Array(Float64))';
SELECT [1, 2, 3]::QBit(Float32, 3)::Array(Float64);

SELECT 'Dimension not a multiple of 8 (no trailing padding elements)';
SELECT length([1, 2, 3, 4, 5, 6, 7, 8, 9]::QBit(Float64, 9)::Array(Float64));
SELECT [1, 2, 3, 4, 5, 6, 7, 8, 9]::QBit(Float64, 9)::Array(Float64);

SELECT 'From a table with multiple rows';
DROP TABLE IF EXISTS qbit_to_array_test;
CREATE TABLE qbit_to_array_test (id UInt32, vec QBit(Float32, 4)) ENGINE = Memory;
INSERT INTO qbit_to_array_test VALUES (1, [1, 2, 3, 4]), (2, [4, 3, 2, 1]);
INSERT INTO qbit_to_array_test (id) VALUES (3);
SELECT id, CAST(vec AS Array(Float32)) FROM qbit_to_array_test ORDER BY id;
DROP TABLE qbit_to_array_test;

SELECT 'Non-constant (materialized) QBit column';
SELECT materialize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]::QBit(Float32, 12))::Array(Float32);

SELECT 'BFloat16 reconstruction matches a direct BFloat16 conversion';
SELECT [0.1, 0.2, 0.3]::QBit(BFloat16, 3)::Array(BFloat16) = [0.1, 0.2, 0.3]::Array(BFloat16);

SELECT 'accurateCast';
SELECT accurateCast([1, 2, 3]::QBit(Float32, 3), 'Array(Float64)');

SELECT 'Negative: cannot cast to a multidimensional array';
SELECT [1, 2, 3]::QBit(Float32, 3)::Array(Array(Float32)); -- { serverError TYPE_MISMATCH }
