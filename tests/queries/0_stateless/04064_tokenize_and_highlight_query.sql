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
