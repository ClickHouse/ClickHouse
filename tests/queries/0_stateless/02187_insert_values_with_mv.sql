CREATE TABLE IF NOT EXISTS data_a_02187 (a Int64) ENGINE=Memory;
CREATE TABLE IF NOT EXISTS data_b_02187 (a Int64) ENGINE=Memory;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv1 TO data_b_02187 AS Select sleepEachRow(0.05) as a FROM data_a_02187;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv2 TO data_b_02187 AS Select sleepEachRow(0.05) as a FROM data_a_02187;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv3 TO data_b_02187 AS Select sleepEachRow(0.05) as a FROM data_a_02187;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv4 TO data_b_02187 AS Select sleepEachRow(0.05) as a FROM data_a_02187;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv5 TO data_b_02187 AS Select sleepEachRow(0.05) as a FROM data_a_02187;

-- INSERT USING VALUES
INSERT INTO data_a_02187 VALUES (1);
-- INSERT USING TABLE
INSERT INTO data_a_02187 SELECT * FROM system.one;
SYSTEM FLUSH LOGS;

SELECT 'VALUES', query_duration_ms >= 250
FROM system.query_log
WHERE
      current_database = currentDatabase()
  AND event_date >= yesterday()
  AND query LIKE '-- INSERT USING VALUES%'
  AND type = 'QueryFinish'
LIMIT 1;

SELECT 'TABLE', query_duration_ms >= 250
FROM system.query_log
WHERE
        current_database = currentDatabase()
  AND event_date >= yesterday()
  AND query LIKE '-- INSERT USING VALUES%'
  AND type = 'QueryFinish'
LIMIT 1;

WITH
    (
        SELECT initial_query_id
        FROM system.query_log
        WHERE
                current_database = currentDatabase()
          AND event_date >= yesterday()
          AND query LIKE '-- INSERT USING VALUES%'
        LIMIT 1
    ) AS q_id
SELECT 'VALUES', view_duration_ms >= 50
FROM system.query_views_log
WHERE initial_query_id = q_id;

WITH
(
    SELECT initial_query_id
    FROM system.query_log
    WHERE
            current_database = currentDatabase()
      AND event_date >= yesterday()
      AND query LIKE '-- INSERT USING TABLE%'
    LIMIT 1
) AS q_id
SELECT 'TABLE', view_duration_ms >= 50
FROM system.query_views_log
WHERE initial_query_id = q_id;
