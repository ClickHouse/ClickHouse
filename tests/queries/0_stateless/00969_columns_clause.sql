DROP TABLE IF EXISTS ColumnsClauseTest;

CREATE TABLE ColumnsClauseTest (product_price Int64, product_weight Int16, amount Int64) Engine=TinyLog;
INSERT INTO ColumnsClauseTest VALUES (100, 10, 324), (120, 8, 23);
SELECT COLUMNS('product.*') from ColumnsClauseTest ORDER BY product_price;

DROP TABLE ColumnsClauseTest;

SELECT number, COLUMNS('') FROM numbers(2);
SELECT number, COLUMNS('ber') FROM numbers(2); -- It works for unanchored regular expressions.
SELECT number, COLUMNS('x') FROM numbers(2);
SELECT COLUMNS('') FROM numbers(2);

SELECT COLUMNS('x') FROM numbers(10) WHERE number > 5; -- { serverError EMPTY_LIST_OF_COLUMNS_QUERIED }

SELECT * FROM numbers(2) WHERE NOT ignore();
SELECT * FROM numbers(2) WHERE NOT ignore(*);
SELECT * FROM numbers(2) WHERE NOT ignore(COLUMNS('.+'));
SELECT * FROM numbers(2) WHERE NOT ignore(COLUMNS('x'));
SELECT COLUMNS('n') + COLUMNS('u') FROM system.numbers LIMIT 2;

SELECT COLUMNS('n') + COLUMNS('u') FROM (SELECT 1 AS a, 2 AS b); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT COLUMNS('a') + COLUMNS('b') FROM (SELECT 1 AS a, 2 AS b);
SELECT COLUMNS('a') + COLUMNS('a') FROM (SELECT 1 AS a, 2 AS b);
SELECT COLUMNS('b') + COLUMNS('b') FROM (SELECT 1 AS a, 2 AS b);
SELECT COLUMNS('a|b') + COLUMNS('b') FROM (SELECT 1 AS a, 2 AS b); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT plus(COLUMNS('^(a|b)$')) FROM (SELECT 1 AS a, 2 AS b);
