-- Tags: distributed

SET max_execution_speed = 1000000;
SET timeout_before_checking_execution_speed = 0;
SET max_block_size = 100;

SET log_queries=1;

CREATE TEMPORARY TABLE times (t DateTime);

INSERT INTO times SELECT now();
SELECT count('special query for 01290_max_execution_speed_distributed') FROM remote('127.0.0.{2,3}', numbers(1000000));
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;

-- Check that the query was also throttled on "remote" servers.
SYSTEM FLUSH LOGS;
SELECT DISTINCT query_duration_ms >= 500
FROM system.query_log
WHERE
    current_database = currentDatabase() AND
    event_date >= yesterday() AND
    query LIKE '%special query for 01290_max_execution_speed_distributed%' AND
    query NOT LIKE '%system.query_log%' AND
    type = 2;
