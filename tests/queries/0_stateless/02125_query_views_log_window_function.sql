set enable_analyzer = 0;
set allow_experimental_window_view = 1;

CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
CREATE WINDOW VIEW wv Engine Memory as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id;

-- INSERT INTO wv
INSERT INTO data VALUES(1,now());

SYSTEM FLUSH LOGS;

-- { echo }

WITH
    (
        SELECT initial_query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND event_date >= yesterday()
          AND query LIKE '-- INSERT INTO wv%'
        LIMIT 1
    ) AS q_id
SELECT view_name, view_type, view_query, read_rows, read_bytes, written_rows, written_bytes
FROM system.query_views_log
WHERE initial_query_id = q_id FORMAT Vertical;

WITH
    (
        SELECT initial_query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND event_date >= yesterday()
          AND query LIKE '-- INSERT INTO wv%'
        LIMIT 1
    ) AS q_id
SELECT views
FROM system.query_log
WHERE initial_query_id = q_id
  AND type = 'QueryFinish'
FORMAT Vertical;
