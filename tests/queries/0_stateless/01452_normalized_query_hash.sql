SELECT normalizedQueryHash('SELECT 1') = normalizedQueryHash('SELECT 2');
SELECT normalizedQueryHash('SELECT  1') != normalizedQueryHash('SELECT  1, 1, 1');
SELECT normalizedQueryHash('SELECT 1, 1, 1, /* Hello */ \'abc\'') = normalizedQueryHash('SELECT 2, 3');
SELECT normalizedQueryHash('[1, 2, 3]') = normalizedQueryHash('[1, ''x'']');
SELECT normalizedQueryHash('[1, 2, 3, x]') != normalizedQueryHash('[1, x]');
SELECT normalizedQueryHash('SELECT 1 AS `xyz`') != normalizedQueryHash('SELECT 1 AS `abc`');
SELECT normalizedQueryHash('SELECT 1 AS xyz111') = normalizedQueryHash('SELECT 2 AS xyz234');
SELECT normalizedQueryHash('SELECT $doc$VALUE$doc$ AS `xyz`') != normalizedQueryHash('SELECT $doc$VALUE$doc$ AS `abc`');
SELECT normalizedQueryHash('SELECT $doc$VALUE$doc$ AS xyz111') = normalizedQueryHash('SELECT $doc$VALUE$doc$ AS xyz234');

-- The sign of numeric literals inside a comma-separated list must not change the hash:
-- normalizedQueryHash folds a +/- right after a comma the same way normalizeQuery does
-- (see normalizeQueryToPODArray in the same file). Regression test for issue #108996.
SELECT normalizedQueryHash('[1, 2, 3]') = normalizedQueryHash('[1, -2, 3]');
SELECT normalizedQueryHash('[1, 2, 3]') = normalizedQueryHash('[1, +2, 3]');
SELECT normalizedQueryHash('SELECT x IN (1, 2, 3)') = normalizedQueryHash('SELECT x IN (1, -2, 3)');
SELECT normalizedQueryHash('f(1, 2, 3)') = normalizedQueryHash('f(1, -2, -3)');
-- Negative control: a genuinely different shape must still differ (no over-collapse).
SELECT normalizedQueryHash('[1, 2, 3]') != normalizedQueryHash('[1, 2, 3, x]');


