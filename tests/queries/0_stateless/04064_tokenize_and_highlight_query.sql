SELECT tokenizeQuery('SELECT 1');
SELECT tokenizeQuery('');
SELECT tokenizeQuery('SELECT number FROM numbers(10) WHERE number > 5');
SELECT highlightQuery('SELECT 1');
SELECT highlightQuery('');
SELECT highlightQuery('SELECT number FROM numbers(10) WHERE number > 5');
SELECT tokenizeQuery('SELECT \'hello world\'');
SELECT highlightQuery('SELECT \'hello world\'');
SELECT highlightQuery('SELECT \'hello\\\\world\'');
SELECT highlightQuery('SELECT name FROM t WHERE name LIKE \'%test\\_name%\'');
SELECT highlightQuery('SELECT name FROM t WHERE name REGEXP \'(test|foo).*bar$\'');

-- Multiple rows via VALUES (exercises input_rows_count > 1)
SELECT query, tokenizeQuery(query) FROM VALUES('query String', ('SELECT 1'), ('INSERT INTO t VALUES (1, 2)'), ('SELECT name FROM t WHERE id > 5'));
SELECT query, highlightQuery(query) FROM VALUES('query String', ('SELECT 1'), ('INSERT INTO t VALUES (1, 2)'), ('SELECT name FROM t WHERE id > 5'));

-- Multiple rows via subquery with numbers
SELECT q, tokenizeQuery(q) FROM (SELECT concat('SELECT ', toString(number)) AS q FROM numbers(5));
SELECT q, highlightQuery(q) FROM (SELECT concat('SELECT ', toString(number)) AS q FROM numbers(5));

-- Readings that are taken back: ALL and DISTINCT as identifiers, and a single number in an INTERVAL string.
SELECT highlightQuery('SELECT f(ALL), f(DISTINCT), f(ALL, x), f(DISTINCT x), count(ALL x)');
SELECT highlightQuery('SELECT quantile(0.5)(ALL), quantile(0.5)(DISTINCT), quantile(0.5)(ALL x)');
SELECT highlightQuery('SELECT INTERVAL \'1\' DAY, INTERVAL \'1 DAY\', INTERVAL \'1:30\' HOUR TO MINUTE, INTERVAL 1 DAY');
