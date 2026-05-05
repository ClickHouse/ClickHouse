-- Regression test for the recursive CTE join-index optimization with
-- `max_query_size = 0`. The parser treats 0 as the "unlimited" sentinel
-- (see `Lexer::nextToken`), so the size guard on the generated
-- `additional_table_filters` must skip the check rather than treat it as
-- a hard zero-byte limit (which would silently disable the optimization
-- for any query running under that configuration).

SET enable_analyzer = 1;
SET max_query_size = 0;

DROP TABLE IF EXISTS edges;
CREATE TABLE edges
(
    from_id UInt64,
    to_id UInt64
) ENGINE = MergeTree ORDER BY from_id SETTINGS index_granularity = 8192;

INSERT INTO edges SELECT number, number + 1 FROM numbers(10);
INSERT INTO edges SELECT number + 1000, number + 1000000 FROM numbers(99000);

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

-- The optimization must still apply under `max_query_size = 0`: without it,
-- each of ~10 recursion steps would scan all ~99010 rows, totaling ~1M.
SELECT
    read_rows < 10000 AS read_rows_ok
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%RECURSIVE traverse AS%INNER JOIN traverse%'
    AND query NOT LIKE '%system.query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE edges;
