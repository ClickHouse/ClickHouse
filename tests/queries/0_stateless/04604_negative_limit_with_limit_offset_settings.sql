-- A negative LIMIT selects the last |n| rows; the `limit`/`offset` settings then apply on top of
-- that result. They must be applied as an outer cap step and not arithmetically folded into the
-- negative limit expression (the fold assumes a non-negative limit and would produce `LIMIT 0`).

SELECT '-- no settings: last 3 rows';
SELECT number FROM numbers(10) LIMIT -3;

SELECT '-- limit 5 does not truncate the 3-row result';
SET limit = 5;
SELECT number FROM numbers(10) LIMIT -3;

SELECT '-- limit 2 keeps the first 2 of the last 3 rows';
SET limit = 2;
SELECT number FROM numbers(10) LIMIT -3;

SET limit = 0;
SELECT '-- offset 1 skips the first of the last 3 rows';
SET offset = 1;
SELECT number FROM numbers(10) LIMIT -3;

SET offset = 0;
SELECT '-- offset 1, limit 1 returns the middle of the last 3 rows';
SET offset = 1;
SET limit = 1;
SELECT number FROM numbers(10) LIMIT -3;

SET offset = 0;
SET limit = 0;
SELECT '-- offset 5 skips more rows than the negative limit returns';
SET offset = 5;
SELECT number FROM numbers(10) LIMIT -3;

SET offset = 0;
SELECT '-- explicit OFFSET clause applies before the negative limit, settings after it';
SET limit = 2;
SELECT number FROM numbers(10) LIMIT -3 OFFSET 2;
