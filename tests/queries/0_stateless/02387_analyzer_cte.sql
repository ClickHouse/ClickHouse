SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO test_table VALUES (0, 'Value');

WITH cte_subquery AS (SELECT 1) SELECT * FROM cte_subquery;

SELECT '--';

WITH cte_subquery AS (SELECT * FROM test_table) SELECT * FROM cte_subquery;

SELECT '--';

WITH cte_subquery AS (SELECT 1 UNION DISTINCT SELECT 1) SELECT * FROM cte_subquery;

SELECT '--';

WITH cte_subquery AS (SELECT * FROM test_table UNION DISTINCT SELECT * FROM test_table) SELECT * FROM cte_subquery;

DROP TABLE test_table;
