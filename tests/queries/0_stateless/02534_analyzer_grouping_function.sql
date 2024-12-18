SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, 'Value');

-- { echoOn }

EXPLAIN QUERY TREE SELECT grouping(id), grouping(value) FROM test_table GROUP BY id, value;

SELECT grouping(id) AS grouping_id, grouping(value) AS grouping_value, id, value FROM test_table
GROUP BY id, value ORDER BY grouping_id, grouping_value;

EXPLAIN QUERY TREE SELECT grouping(id), grouping(value) FROM test_table GROUP BY ROLLUP (id, value);

SELECT grouping(id) AS grouping_id, grouping(value) AS grouping_value, id, value FROM test_table
GROUP BY ROLLUP (id, value) ORDER BY grouping_id, grouping_value;

EXPLAIN QUERY TREE SELECT grouping(id), grouping(value) FROM test_table GROUP BY CUBE (id, value);

SELECT grouping(id) AS grouping_id, grouping(value) AS grouping_value, id, value FROM test_table
GROUP BY CUBE (id, value) ORDER BY grouping_id, grouping_value;

EXPLAIN QUERY TREE SELECT grouping(id), grouping(value) FROM test_table GROUP BY GROUPING SETS (id, value);

SELECT grouping(id) AS grouping_id, grouping(value) AS grouping_value, id, value FROM test_table
GROUP BY GROUPING SETS (id, value) ORDER BY grouping_id, grouping_value;

EXPLAIN QUERY TREE SELECT grouping(id), grouping(value) FROM test_table GROUP BY GROUPING SETS ((id), (value));

SELECT grouping(id) AS grouping_id, grouping(value) AS grouping_value, id, value FROM test_table
GROUP BY GROUPING SETS ((id), (value)) ORDER BY grouping_id, grouping_value;

-- { echoOff }

DROP TABLE test_table;
