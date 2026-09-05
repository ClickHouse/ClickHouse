-- A fractional LIMIT selects a fraction of the result; the `limit`/`offset` settings then apply on
-- top of that result. They must be applied as an outer cap step and not arithmetically folded into
-- the fractional limit expression (the fold assumes an integer row count: its zero-guard
-- `offset >= 0.5` would drop all rows, and `least(0.5, limit)` would ignore the cap).

SELECT '-- no settings: half of 10 rows';
SELECT number FROM numbers(10) LIMIT 0.5;

SELECT '-- limit 2 keeps the first 2 of the 5 rows';
SET limit = 2;
SELECT number FROM numbers(10) LIMIT 0.5;

SET limit = 0;
SELECT '-- offset 3 skips 3 of the 5 rows';
SET offset = 3;
SELECT number FROM numbers(10) LIMIT 0.5;

SET offset = 0;
SELECT '-- offset 2, limit 2 returns rows 2 and 3 of the 5';
SET offset = 2;
SET limit = 2;
SELECT number FROM numbers(10) LIMIT 0.5;
