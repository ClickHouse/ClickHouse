-- Test that aggregate_functions_null_for_empty works correctly with aggregate functions
-- that return types which cannot be inside Nullable (e.g., Array, Map).

SET aggregate_functions_null_for_empty = 1;

SELECT groupArray(number) FROM numbers(0);
SELECT groupArray(number) FROM numbers(5);

SELECT groupUniqArray(number) FROM numbers(0);

SELECT sumMap(map('a', number)) FROM numbers(0);
SELECT sumMap(map('a', number)) FROM numbers(5);

-- Regular aggregate functions that return Nullable-compatible types should still return NULL for empty.
SELECT sum(number) FROM numbers(0);
SELECT count(number) FROM numbers(0);
SELECT min(number) FROM numbers(0);
SELECT max(number) FROM numbers(0);
SELECT avg(number) FROM numbers(0);
