SET enable_analyzer = 1;

SELECT 'Aliases to constants';

SELECT 1 as a, a;
SELECT (c + 1) as d, (a + 1) as b, 1 AS a, (b + 1) as c, d;

WITH 1 as a SELECT a;
WITH a as b SELECT 1 as a, b;

SELECT 1 AS x, x, x + 1;
SELECT x, x + 1, 1 AS x;
SELECT x, 1 + (2 + (3 AS x));

SELECT a AS b, b AS a; -- { serverError CYCLIC_ALIASES }

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT 'Aliases to columns';

SELECT id_alias_2, id AS id_alias, id_alias as id_alias_2 FROM test_table;
SELECT id_1, value_1, id as id_1, value as value_1 FROM test_table;

WITH value_1 as value_2, id_1 as id_2, id AS id_1, value AS value_1 SELECT id_2, value_2 FROM test_table;

SELECT (id + b) AS id, id as b FROM test_table; -- { serverError CYCLIC_ALIASES }
SELECT (1 + b + 1 + id) AS id, b as c, id as b FROM test_table; -- { serverError CYCLIC_ALIASES }

SELECT 'Alias conflict with identifier inside expression';

SELECT id AS id FROM test_table;
SELECT (id + 1) AS id FROM test_table;
SELECT (id + 1 + 1 + 1 + id) AS id FROM test_table;

SELECT 'Alias setting prefer_column_name_to_alias';

WITH id AS value SELECT value FROM test_table;

SET prefer_column_name_to_alias = 1;
WITH id AS value SELECT value FROM test_table;
SET prefer_column_name_to_alias = 0;

DROP TABLE test_table;

WITH path('clickhouse.com/a/b/c') AS x SELECT x AS path;
