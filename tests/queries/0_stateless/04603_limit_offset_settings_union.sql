-- The `limit`/`offset` settings cap the merged result of a set operation exactly once, above the
-- union. They must not be folded into each branch: per-branch folding caps every branch separately
-- (returning up to N rows per branch instead of N rows total) and corrupts negative LIMIT
-- expressions into `LIMIT 0`. All branches produce constant values because the interleaving order
-- of UNION ALL output is not deterministic.

SET limit = 10;

SELECT '-- cap 10 over 8 + 8 rows: 10 rows total, not 16';
SELECT 1 FROM numbers(8) UNION ALL SELECT 1 FROM numbers(8);

SELECT '-- branch limits below the cap stay intact: 3 + 2 rows';
SELECT 2 FROM numbers(20) LIMIT 3 UNION ALL SELECT 2 FROM numbers(20) LIMIT 2;

SET limit = 4;

SELECT '-- cap 4 over branch limits 3 + 2: 4 rows';
SELECT 3 FROM numbers(20) LIMIT 3 UNION ALL SELECT 3 FROM numbers(20) LIMIT 2;

SET limit = 10;

SELECT '-- negative branch limits below the cap: last 3 + last 2 rows';
SELECT 4 FROM numbers(10) LIMIT -3 UNION ALL SELECT 4 FROM numbers(10) LIMIT -2;

SET limit = 4;

SELECT '-- cap 4 over negative branch limits 3 + 3: 4 rows';
SELECT 5 FROM numbers(10) LIMIT -3 UNION ALL SELECT 5 FROM numbers(10) LIMIT -3;

SELECT '-- offset 3, limit 2 over 3 + 2 rows: skip 3 of the merged stream, return 2';
SET limit = 2;
SET offset = 3;
SELECT 6 FROM numbers(20) LIMIT 3 UNION ALL SELECT 6 FROM numbers(20) LIMIT 2;

SET limit = 0;
SET offset = 0;
SELECT '-- offset 3 alone over 3 + 2 rows: return 2';
SET offset = 3;
SELECT 7 FROM numbers(20) LIMIT 3 UNION ALL SELECT 7 FROM numbers(20) LIMIT 2;
