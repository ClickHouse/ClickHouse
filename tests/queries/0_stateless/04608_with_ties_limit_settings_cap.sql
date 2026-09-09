-- The `limit` setting is a hard cap on the number of result rows. LIMIT ... WITH TIES may extend
-- the query's own result past its LIMIT boundary, but the setting still caps the final result and
-- must not be folded into the WITH TIES limit (folding would compute ties at the capped boundary
-- and return more rows than the setting allows).

SELECT '-- no settings: LIMIT 5 WITH TIES extends to the whole tied group, 6 rows';
SELECT intDiv(number, 3) AS k FROM numbers(10) ORDER BY k LIMIT 5 WITH TIES;

SELECT '-- limit 4 caps the 6-row tied result at 4 rows';
SET limit = 4;
SELECT intDiv(number, 3) AS k FROM numbers(10) ORDER BY k LIMIT 5 WITH TIES;

SET limit = 0;
SELECT '-- offset 2 skips 2 of the 6-row tied result';
SET offset = 2;
SELECT intDiv(number, 3) AS k FROM numbers(10) ORDER BY k LIMIT 5 WITH TIES;
