SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String,
    value_array Array(UInt64)
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value_0', [1,2,3]);

-- { echoOn }

SELECT * FROM test_table ARRAY JOIN value_array;

SELECT '--';

SELECT *, value_array_element FROM test_table ARRAY JOIN value_array AS value_array_element;

SELECT '--';

SELECT *, value_array FROM test_table ARRAY JOIN value_array AS value_array;

SELECT '--';

SELECT *, value_array FROM test_table ARRAY JOIN [4,5,6] AS value_array;

SELECT '--';

SELECT *, value_array, value_element FROM test_table ARRAY JOIN value_array, [4,5,6] AS value_element;

SELECT '--';

SELECT * FROM (SELECT [dummy, dummy] AS dummy FROM system.one) AS subquery ARRAY JOIN dummy INNER JOIN system.one USING (dummy);

SELECT '--';

SELECT * FROM (SELECT [0] AS id) AS subquery_1 ARRAY JOIN id INNER JOIN (SELECT 0 AS id) AS subquery_2 USING (id);

SELECT '--';

SELECT * FROM (SELECT [1] AS id) AS subquery_1 ARRAY JOIN id INNER JOIN (SELECT 0 AS id) AS subquery_2 USING (id);

SELECT '--';

SELECT * FROM (SELECT [0] AS id) AS subquery_1 ARRAY JOIN id INNER JOIN (SELECT 1 AS id) AS subquery_2 USING (id);

SELECT '--';

SELECT * FROM (SELECT [1] AS id) AS subquery_1 ARRAY JOIN id INNER JOIN (SELECT 1 AS id) AS subquery_2 USING (id);

SELECT '--';

SELECT * FROM (SELECT [5] AS id) AS subquery_1 ARRAY JOIN [1,2,3] AS id INNER JOIN (SELECT 1 AS id) AS subquery_2 USING (id);

SELECT '--';

SELECT * FROM (SELECT [0] AS id) AS subquery ARRAY JOIN id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [1] AS id) AS subquery ARRAY JOIN id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [0] AS id) AS subquery ARRAY JOIN id AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [1] AS id) AS subquery ARRAY JOIN id AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT *, id FROM (SELECT [0] AS id) AS subquery ARRAY JOIN id AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT *, id FROM (SELECT [1] AS id) AS subquery ARRAY JOIN id AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [0] AS value) AS subquery ARRAY JOIN value AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [1] AS value) AS subquery ARRAY JOIN value AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT *, id FROM (SELECT [0] AS value) AS subquery ARRAY JOIN value AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT *, id FROM (SELECT [1] AS value) AS subquery ARRAY JOIN value AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [0] AS id) AS subquery ARRAY JOIN [0] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [0] AS id) AS subquery ARRAY JOIN [1] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT *, id FROM (SELECT [0] AS id) AS subquery ARRAY JOIN [0] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT *, id FROM (SELECT [0] AS id) AS subquery ARRAY JOIN [1] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [5] AS id) AS subquery ARRAY JOIN [0] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [5] AS id) AS subquery ARRAY JOIN [1] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT *, id FROM (SELECT [5] AS id) AS subquery ARRAY JOIN [0] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT *, id FROM (SELECT [5] AS id) AS subquery ARRAY JOIN [1] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [5] AS id_array) AS subquery ARRAY JOIN id_array, [0] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [[0]] AS id) AS subquery ARRAY JOIN id AS id_nested_array ARRAY JOIN id_nested_array AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT *, id FROM (SELECT [[0]] AS id) AS subquery ARRAY JOIN id AS id_nested_array ARRAY JOIN id_nested_array AS id INNER JOIN test_table USING (id);

-- { echoOff }

DROP TABLE test_table;
