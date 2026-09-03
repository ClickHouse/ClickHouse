SELECT '-- Negative tests';

SELECT arrayDotProduct([1, 2]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayDotProduct([1, 2], 'abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayDotProduct('abc', [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayDotProduct([1, 2], ['abc', 'def']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayDotProduct([1, 2], [3, 4, 5]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT dotProduct([1, 2], (3, 4, 5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Tests';
SELECT '   -- Array';
SELECT [1, 2, 3]::Array(UInt8) AS x, [4, 5, 6]::Array(UInt8) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT [1, 2, 3]::Array(UInt16) AS x, [4, 5, 6]::Array(UInt16) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT [1, 2, 3]::Array(UInt32) AS x, [4, 5, 6]::Array(UInt32) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT [1, 2, 3]::Array(UInt64) AS x, [4, 5, 6]::Array(UInt64) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT [-1, -2, -3]::Array(Int8) AS x, [4, 5, 6]::Array(Int8) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT [-1, -2, -3]::Array(Int16) AS x, [4, 5, 6]::Array(Int16) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT [-1, -2, -3]::Array(Int32) AS x, [4, 5, 6]::Array(Int32) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT [-1, -2, -3]::Array(Int64) AS x, [4, 5, 6]::Array(Int64) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT [1, 2, 3]::Array(Float32) AS x, [4, 5, 6]::Array(Float32) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT [1, 2, 3]::Array(Float64) AS x, [4, 5, 6]::Array(Float64) AS y, dotProduct(x, y) AS res, toTypeName(res);
-- empty arrays
SELECT []::Array(Float32) AS x, []::Array(Float32) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT []::Array(UInt8) AS x, []::Array(UInt8) AS y, dotProduct(x, y) AS res, toTypeName(res);

SELECT '   -- Tuple';
SELECT (1::UInt8, 2::UInt8, 3::UInt8) AS x, (4::UInt8, 5::UInt8, 6::UInt8) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT (1::UInt16, 2::UInt16, 3::UInt16) AS x, (4::UInt16, 5::UInt16, 6::UInt16) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT (1::UInt32, 2::UInt32, 3::UInt32) AS x, (4::UInt32, 5::UInt32, 6::UInt32) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT (1::UInt64, 2::UInt64, 3::UInt64) AS x, (4::UInt64, 5::UInt64, 6::UInt64) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT (-1::Int8, -2::Int8, -3::Int8) AS x, (4::Int8, 5::Int8, 6::Int8) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT (-1::Int16, -2::Int16, -3::Int16) AS x, (4::Int16, 5::Int16, 6::Int16) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT (-1::Int32, -2::Int32, -3::Int32) AS x, (4::Int32, 5::Int32, 6::Int32) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT (-1::Int64, -2::Int64, -3::Int64) AS x, (4::Int64, 5::Int64, 6::Int64) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT (1::Float32, 2::Float32, 3::Float32) AS x, (4::Float32, 5::Float32, 6::Float32) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT (1::Float64, 2::Float64, 3::Float64) AS x, (4::Float64, 5::Float64, 6::Float64) AS y, dotProduct(x, y) AS res, toTypeName(res);

SELECT '-- Non-const argument';
SELECT materialize([1::UInt8, 2::UInt8, 3::UInt8]) AS x, [4::UInt8, 5::UInt8, 6::UInt8] AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT materialize([]::Array(Float32)) AS x, []::Array(Float32) AS y, dotProduct(x, y) AS res, toTypeName(res);
SELECT materialize([]::Array(UInt8)) AS x, []::Array(UInt8) AS y, dotProduct(x, y) AS res, toTypeName(res);

SELECT ' -- Array with mixed element arguments types (result type is the supertype)';
SELECT [1::UInt16, 2::UInt8, 3::Float32] AS x, [4::Int16, 5::Float32, 6::UInt8] AS y, dotProduct(x, y) AS res, toTypeName(res);

SELECT ' -- Tuple with mixed element arguments types';
SELECT (1::UInt16, 2::UInt8, 3::Float32) AS x, (4::Int16, 5::Float32, 6::UInt8) AS y, dotProduct(x, y) AS res, toTypeName(res);

SELECT '-- Aliases';
SELECT scalarProduct([1, 2, 3], [4, 5, 6]);
SELECT scalarProduct((1, 2, 3), (4, 5, 6));
SELECT arrayDotProduct([1, 2, 3], [4, 5, 6]); -- actually no alias but the internal function for arrays

SELECT '-- Tests that trigger special paths';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab(id UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (0, [0.0, 1.0, 2.0, 3.0, 0.0, 1.0, 2.0, 3.0, 0.0, 1.0, 2.0, 3.0, 0.0, 1.0, 2.0, 3.0, 0.0, 1.0, 2.0]) (1, [5.0, 1.0, 2.0, 3.0, 5.0, 1.0, 2.0, 3.0, 5.0, 1.0, 2.0, 3.0, 5.0, 1.0, 2.0, 3.0, 5.0, 1.0, 2.0]);
SELECT '   -- non-const / non-const';
SELECT id, arrayDotProduct(vec, vec) FROM tab ORDER BY id;
SELECT id, arrayDotProduct(vec::Array(Float64), vec::Array(Float64)) FROM tab ORDER BY id;
SELECT id, arrayDotProduct(vec::Array(UInt32), vec::Array(UInt32)) FROM tab ORDER BY id;
SELECT '   -- const / non-const';
SELECT id, arrayDotProduct([5.0, 2.0, 2.0, 3.0, 5.0, 1.0, 2.0, 3.0, 5.0, 1.0, 2.0, 3.0, 5.0, 1.0, 2.0, 3.0, 5.0, 1.0, 2.0]::Array(Float32), vec) FROM tab ORDER BY id;
SELECT id, arrayDotProduct([5.0, 2.0, 2.0, 3.0, 5.0, 1.0, 2.0, 3.0, 5.0, 1.0, 2.0, 3.0, 5.0, 1.0, 2.0, 3.0, 5.0, 1.0, 2.0]::Array(Float64), vec) FROM tab ORDER BY id;
SELECT id, arrayDotProduct([5, 2, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2, 3, 5, 1, 2]::Array(UInt32), vec) FROM tab ORDER BY id;
DROP TABLE tab;
