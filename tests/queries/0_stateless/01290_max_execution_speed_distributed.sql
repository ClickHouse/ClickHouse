-- Tags: distributed

SET max_execution_speed = 1000000;
SET timeout_before_checking_execution_speed = 0;
SET max_block_size = 100;

SET log_queries=1;

CREATE TEMPORARY TABLE times (t DateTime);

INSERT INTO times SELECT now();
SELECT count() FROM remote('127.0.0.{2,3}', numbers(1000000)) SETTINGS log_comment='01290_8ca5d52f-8582-4ee3-8674-351c76d67b8c';
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;

-- Check that the query was also throttled on "remote" servers.
SYSTEM FLUSH LOGS;
SELECT COUNT()
FROM system.query_log
WHERE
    current_database = currentDatabase() AND
    event_date >= yesterday() AND
    log_comment = '01290_8ca5d52f-8582-4ee3-8674-351c76d67b8c' AND
    type = 'QueryFinish' AND
    query_duration_ms >= 500
SETTINGS max_threads = 0;
