-- Tags: no-fasttest

DROP TABLE IF EXISTS current_failed_query_metrics;
DROP TABLE IF EXISTS t_async_insert_02474_1;

SET log_queries = 1;

CREATE TABLE current_failed_query_metrics (event LowCardinality(String), value UInt64) ENGINE = Memory();
CREATE TABLE t_async_insert_02474_1 (id UInt32, s String) ENGINE = Memory;

-- Failed insert in execution
SET async_insert = 1;
INSERT INTO t_async_insert_02474_1 SELECT throwIf(1),''; -- { serverError 395 }
INSERT INTO t_async_insert_02474_1 SELECT throwIf(1),''; -- { serverError 395 }
INSERT INTO t_async_insert_02474_1 SELECT throwIf(1),''; -- { serverError 395 }
INSERT INTO t_async_insert_02474_1 SELECT throwIf(1),''; -- { serverError 395 }

SYSTEM FLUSH LOGS;


SELECT current_value - previous_value
FROM (
    SELECT event, value as current_value FROM system.events WHERE event like 'FailedAsyncInsertQuery'
) AS previous
ALL LEFT JOIN (
    SELECT event, value as previous_value FROM current_failed_query_metrics
) AS current
on previous.event = current.event;

DROP TABLE IF EXISTS current_failed_query_metrics;
DROP TABLE IF EXISTS t_async_insert_02474_1;