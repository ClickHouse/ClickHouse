-- https://github.com/ClickHouse/ClickHouse/issues/103681
-- normalizeQuery used to drop unary +/- before a non-literal (identifier or function call) that follows a comma in a literal sequence

SELECT normalizeQuery('1, -col');
SELECT normalizeQuery('1, +col');
SELECT normalizeQuery('1, -x, 3');
SELECT normalizeQuery('1, -CAST(2 AS UInt8)');
SELECT normalizeQuery('SELECT * FROM t WHERE x IN (1, -col, 2)');
SELECT normalizeQuery('SELECT * FROM t WHERE x IN (1, col, 2)');

SELECT normalizeQuery('1, -col') = normalizeQuery('1, col');
SELECT normalizeQuery('SELECT * FROM t WHERE x IN (1, -col, 2)') = normalizeQuery('SELECT * FROM t WHERE x IN (1, col, 2)');

SELECT normalizeQuery('1, - -x');

SELECT normalizeQuery('1, -2, 3');
SELECT normalizeQuery('1, - -2, 3');

SELECT normalizedQueryHash('1, -2, 3', false) = normalizedQueryHash('1, 2, 3', false);
SELECT normalizedQueryHash('1, -x, 3', false) = normalizedQueryHash('1, x, 3', false);
