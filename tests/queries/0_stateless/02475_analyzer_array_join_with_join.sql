SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value_0');
INSERT INTO test_table VALUES (1, 'Value_1');

SELECT * FROM (SELECT [dummy, dummy] AS dummy FROM system.one) AS subquery ARRAY JOIN dummy INNER JOIN system.one USING (dummy);

SELECT '--';

SELECT * FROM (SELECT [0] AS dummy) AS subquery_1 ARRAY JOIN dummy INNER JOIN system.one USING (dummy);

SELECT '--';

SELECT * FROM (SELECT [1] AS dummy) AS subquery_1 ARRAY JOIN dummy INNER JOIN system.one USING (dummy);

SELECT '--';

SELECT * FROM (SELECT [0] AS dummy) AS subquery_1 ARRAY JOIN dummy INNER JOIN (SELECT 1 AS dummy) AS subquery_2 USING (dummy);

SELECT '--';

SELECT * FROM (SELECT [1] AS dummy) AS subquery_1 ARRAY JOIN dummy INNER JOIN (SELECT 1 AS dummy) AS subquery_2 USING (dummy);

SELECT '--';

SELECT * FROM (SELECT [0] AS id) AS subquery_1 ARRAY JOIN id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [1] AS id) AS subquery_1 ARRAY JOIN id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [0] AS id) AS subquery_1 ARRAY JOIN id AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [1] AS id) AS subquery_1 ARRAY JOIN id AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [0] AS value) AS subquery_1 ARRAY JOIN value AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [1] AS value) AS subquery_1 ARRAY JOIN value AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [0] AS id) AS subquery_1 ARRAY JOIN [0] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [0] AS id) AS subquery_1 ARRAY JOIN [1] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [5] AS id) AS subquery_1 ARRAY JOIN [0] AS id INNER JOIN test_table USING (id);

SELECT '--';

SELECT * FROM (SELECT [5] AS id) AS subquery_1 ARRAY JOIN [1] AS id INNER JOIN test_table USING (id);

DROP TABLE test_table;
