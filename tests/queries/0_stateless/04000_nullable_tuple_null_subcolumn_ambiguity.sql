-- Tuple element name "null" is forbidden because it conflicts with the subcolumn
-- name used for Nullable null maps, causing ambiguity in subcolumn resolution.

SELECT CAST((1), 'Tuple(null UInt32)'); -- { serverError BAD_ARGUMENTS }
SELECT CAST((1), 'Tuple(null UInt32, b UInt64)'); -- { serverError BAD_ARGUMENTS }

-- Non-Nullable wrapping should also be rejected (the name itself is ambiguous).
CREATE TABLE test_null_tuple (t Tuple(null UInt32)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

-- Normal Tuple element names work fine.
SELECT CAST((1, 2), 'Tuple(a UInt32, b UInt64)');

-- Regression: reading the `.null` subcolumn of `Array(Nullable(T))` must not
-- attempt to construct an `Array(Tuple(null T))` in `Nested::getSubcolumnsOfNested`.
DROP TABLE IF EXISTS test_array_nullable_null_subcolumn;
CREATE TABLE test_array_nullable_null_subcolumn (a3 Array(Nullable(UInt32))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_array_nullable_null_subcolumn VALUES ([1, NULL, 2]);
SELECT a3.null FROM test_array_nullable_null_subcolumn;
DROP TABLE test_array_nullable_null_subcolumn;
