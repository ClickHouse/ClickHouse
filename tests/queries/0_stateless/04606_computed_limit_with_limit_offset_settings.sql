-- A LIMIT given as a constant expression rather than a literal is not arithmetically folded with
-- the `limit`/`offset` settings; the settings are applied as an outer cap step instead. The result
-- must be identical to the equivalent literal LIMIT.

SELECT '-- limit 4 as expression, cap 5: all 4 rows';
SET limit = 5;
SELECT number FROM numbers(10) LIMIT 2 + 2;

SELECT '-- limit 4 as expression, cap 3: first 3 of 4 rows';
SET limit = 3;
SELECT number FROM numbers(10) LIMIT 2 + 2;

SET limit = 0;
SELECT '-- offset 3 over 4 rows: 1 row';
SET offset = 3;
SELECT number FROM numbers(10) LIMIT 2 + 2;
