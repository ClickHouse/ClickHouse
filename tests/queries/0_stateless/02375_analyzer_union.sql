SET enable_analyzer = 0;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO test_table VALUES (0, 'Value');

-- { echoOn }

SELECT 'Union constants';

SELECT 1 UNION ALL SELECT 1;

SELECT '--';

SELECT 1 UNION DISTINCT SELECT 1 UNION ALL SELECT 1;

SELECT '--';

SELECT 1 INTERSECT SELECT 1;

SELECT '--';

SELECT 1 EXCEPT SELECT 1;

SELECT '--';

SELECT id FROM (SELECT 1 AS id UNION ALL SELECT 1);

SELECT 'Union non constants';

SELECT value FROM (SELECT 1 as value UNION ALL SELECT 1 UNION ALL SELECT 1);

SELECT '--';

SELECT id FROM test_table UNION ALL SELECT id FROM test_table;

SELECT '--';

SELECT id FROM test_table UNION DISTINCT SELECT id FROM test_table;

SELECT '--';

SELECT id FROM test_table INTERSECT SELECT id FROM test_table;

SELECT '--';
SELECT id FROM test_table EXCEPT SELECT id FROM test_table;

SELECT '--';

SELECT id FROM (SELECT id FROM test_table UNION ALL SELECT id FROM test_table);

SELECT '--';

SELECT id FROM (SELECT id FROM test_table UNION DISTINCT SELECT id FROM test_table);

SELECT '--';

SELECT id FROM (SELECT id FROM test_table INTERSECT SELECT id FROM test_table);

SELECT '--';

SELECT id FROM (SELECT id FROM test_table EXCEPT SELECT id FROM test_table);

-- { echoOff }

DROP TABLE test_table;
