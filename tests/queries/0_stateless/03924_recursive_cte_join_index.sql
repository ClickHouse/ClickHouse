-- Test that recursive CTEs with JOINs use MergeTree primary key index.
-- Without the optimization, each recursion step scans the entire table.
-- With the optimization, join key values from the working table are pushed
-- as additional_table_filters, enabling index usage.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS edges;
CREATE TABLE edges
(
    from_id UInt64,
    to_id UInt64
) ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 8192;

-- Insert a chain: 0->1->2->...->9
INSERT INTO edges SELECT number, number + 1 FROM numbers(10);

-- Insert many unrelated rows to make index usage measurable.
-- from_id range [1000, 1000000) has no connection to the chain above.
INSERT INTO edges SELECT number + 1000, number + 1000000 FROM numbers(999000);

-- Recursive CTE: traverse the chain starting from 0 using explicit JOIN.
WITH RECURSIVE traverse AS
(
    SELECT to_id AS current_id
    FROM edges
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges AS e
    INNER JOIN traverse AS t ON e.from_id = t.current_id
)
SELECT current_id FROM traverse ORDER BY current_id;

SYSTEM FLUSH LOGS query_log;

-- Check that the total rows read is small (index was used).
-- Without optimization: ~999010 * 10 steps = ~10M rows read.
-- With optimization: a few thousand rows at most.
SELECT
    read_rows < 100000 AS read_rows_ok
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE traverse AS%INNER JOIN traverse%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Also test comma-join syntax (becomes INNER JOIN after CrossToInnerJoinPass).
WITH RECURSIVE traverse2 AS
(
    SELECT to_id AS current_id
    FROM edges
    WHERE from_id = 0
  UNION ALL
    SELECT e.to_id AS current_id
    FROM edges AS e, traverse2 AS t
    WHERE e.from_id = t.current_id
)
SELECT current_id FROM traverse2 ORDER BY current_id;

SYSTEM FLUSH LOGS query_log;

SELECT
    read_rows < 100000 AS read_rows_ok
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE traverse2%FROM edges AS e, traverse2%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE edges;
