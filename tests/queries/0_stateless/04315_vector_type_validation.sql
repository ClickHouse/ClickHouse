-- Tests validation of the Vector(T, N) data type parameters.

SET allow_experimental_vector_type = 1;

SELECT '-- the experimental gate';
SET allow_experimental_vector_type = 0;
CREATE TABLE t_vector_gated (v Vector(Float32, 4)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }
SET allow_experimental_vector_type = 1;

SELECT '-- dimension must be a positive integer';
SELECT CAST([1] AS Vector(Float32, 0)); -- { serverError UNEXPECTED_AST_STRUCTURE }
SELECT CAST([1] AS Vector(Float32, -1)); -- { serverError UNEXPECTED_AST_STRUCTURE }
SELECT CAST([1] AS Vector(Float32, 1.5)); -- { serverError UNEXPECTED_AST_STRUCTURE }

SELECT '-- the value size is bounded (no overflow of element_size * dimension)';
SELECT CAST([1] AS Vector(Float64, 2305843009213693952)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST([1] AS Vector(Float32, 18446744073709551615)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST([1] AS Vector(Float32, 16777216)); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- only float element types are allowed';
SELECT CAST([1] AS Vector(UInt32, 4)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST(['a'] AS Vector(String, 4)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- exactly two arguments';
SELECT CAST([1] AS Vector(Float32)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- binary type encoding round-trip (decodeDataType validates through the same constructor)';
SELECT dynamicType(CAST(CAST([1, 2, 3] AS Vector(Float32, 3)) AS Dynamic));
