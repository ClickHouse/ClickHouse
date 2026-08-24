SELECT normalizedQueryHash('SELECT 1') = normalizedQueryHash('SELECT 2');
SELECT normalizedQueryHash('SELECT  1') != normalizedQueryHash('SELECT  1, 1, 1');
SELECT normalizedQueryHash('SELECT 1, 1, 1, /* Hello */ \'abc\'') = normalizedQueryHash('SELECT 2, 3');
SELECT normalizedQueryHash('[1, 2, 3]') = normalizedQueryHash('[1, ''x'']');
SELECT normalizedQueryHash('[1, 2, 3, x]') != normalizedQueryHash('[1, x]');
SELECT normalizedQueryHash('SELECT 1 AS `xyz`') != normalizedQueryHash('SELECT 1 AS `abc`');
SELECT normalizedQueryHash('SELECT 1 AS xyz111') = normalizedQueryHash('SELECT 2 AS xyz234');
SELECT normalizedQueryHash('SELECT $doc$VALUE$doc$ AS `xyz`') != normalizedQueryHash('SELECT $doc$VALUE$doc$ AS `abc`');
SELECT normalizedQueryHash('SELECT $doc$VALUE$doc$ AS xyz111') = normalizedQueryHash('SELECT $doc$VALUE$doc$ AS xyz234');


