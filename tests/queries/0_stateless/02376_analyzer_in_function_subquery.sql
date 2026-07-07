SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO test_table VALUES (0, 'Value_0'), (1, 'Value_1'), (2, 'Value_2');

DROP TABLE IF EXISTS test_table_for_in;
CREATE TABLE test_table_for_in
(
    id UInt64
) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO test_table_for_in VALUES (0), (1);

-- { echoOn }

SELECT id, value FROM test_table WHERE 1 IN (SELECT 1);

SELECT '--';

SELECT id, value FROM test_table WHERE 0 IN (SELECT 1);

SELECT '--';

SELECT id, value FROM test_table WHERE id IN (SELECT 1);

SELECT '--';

SELECT id, value FROM test_table WHERE id IN (SELECT 2);

SELECT '--';

SELECT id, value FROM test_table WHERE id IN test_table_for_in;

SELECT '--';

SELECT id, value FROM test_table WHERE id IN (SELECT id FROM test_table_for_in);

SELECT '--';

SELECT id, value FROM test_table WHERE id IN (SELECT id FROM test_table_for_in UNION DISTINCT SELECT id FROM test_table_for_in);

SELECT '--';

WITH cte_test_table_for_in AS (SELECT id FROM test_table_for_in) SELECT id, value FROM test_table WHERE id IN cte_test_table_for_in;

SELECT '--';

WITH cte_test_table_for_in AS (SELECT id FROM test_table_for_in) SELECT id, value
FROM test_table WHERE id IN (SELECT id FROM cte_test_table_for_in UNION DISTINCT SELECT id FROM cte_test_table_for_in);

-- { echoOff }

DROP TABLE test_table;
DROP TABLE test_table_for_in;
