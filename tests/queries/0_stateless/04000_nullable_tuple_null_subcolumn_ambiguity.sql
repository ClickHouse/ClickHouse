-- Tuple element name "null" is forbidden because it conflicts with the subcolumn
-- name used for Nullable null maps, causing ambiguity in subcolumn resolution.

SELECT CAST((1), 'Tuple(null UInt32)'); -- { serverError BAD_ARGUMENTS }
SELECT CAST((1), 'Tuple(null UInt32, b UInt64)'); -- { serverError BAD_ARGUMENTS }

-- Non-Nullable wrapping should also be rejected (the name itself is ambiguous).
CREATE TABLE test_null_tuple (t Tuple(null UInt32)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

-- Normal Tuple element names work fine.
SELECT CAST((1, 2), 'Tuple(a UInt32, b UInt64)');
