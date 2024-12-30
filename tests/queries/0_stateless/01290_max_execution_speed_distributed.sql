-- Tags: distributed

SET log_queries=1;

CREATE TEMPORARY TABLE times (t DateTime);

INSERT INTO times SELECT now();
SELECT count('special query for 01290_max_execution_speed_distributed') FROM remote('127.0.0.{2,3}', numbers(1000000))
SETTINGS max_execution_speed = 100000, timeout_before_checking_execution_speed = 0, max_block_size = 100, log_comment='01290_8ca5d52f-8582-4ee3-8674-351c76d67b8c';
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;

-- Check that the query was also throttled on "remote" servers.
SYSTEM FLUSH LOGS;
SELECT COUNT()
FROM system.query_log
WHERE
    current_database = currentDatabase() AND
    event_date >= yesterday() AND
    event_time >= now() - INTERVAL '5 MINUTES' AND -- time limit for tests not marked `long` is 3 minutes, 5 should be more than enough
    log_comment = '01290_8ca5d52f-8582-4ee3-8674-351c76d67b8c' AND
    query LIKE '%special query for 01290_max_execution_speed_distributed%' AND
    query NOT LIKE '%system.query_log%' AND
    type = 'QueryFinish' AND
    query_duration_ms >= 500
SETTINGS max_threads = 0;
