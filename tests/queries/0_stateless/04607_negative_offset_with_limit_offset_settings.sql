-- A negative OFFSET drops the last |n| rows; the `limit`/`offset` settings then apply on top of
-- that result. They must be applied as an outer cap step and not combined with the negative offset
-- expression by addition.

SELECT '-- no settings: all but the last 3 rows';
SELECT number FROM numbers(10) OFFSET -3;

SELECT '-- offset 2 skips the first 2 of the remaining 7 rows';
SET offset = 2;
SELECT number FROM numbers(10) OFFSET -3;

SET offset = 0;
SELECT '-- limit 2 keeps the first 2 of the remaining 7 rows';
SET limit = 2;
SELECT number FROM numbers(10) OFFSET -3;

SELECT '-- offset 2, limit 2 returns rows 2 and 3 of the remaining 7';
SET offset = 2;
SELECT number FROM numbers(10) OFFSET -3;
