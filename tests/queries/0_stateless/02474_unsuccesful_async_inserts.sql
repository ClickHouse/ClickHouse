-- Tags: no-fasttest

DROP TABLE IF EXISTS current_failed_async_query_metrics;
DROP TABLE IF EXISTS t_async_insert_02474_1;

SET log_queries = 1;

CREATE TABLE current_failed_async_query_metrics (event LowCardinality(String), value UInt64) ENGINE = Memory();
CREATE TABLE t_async_insert_02474_1 (id UInt32, s String) ENGINE = Memory;

INSERT INTO current_failed_async_query_metrics 
SELECT event, value
FROM system.events
WHERE event in ('FailedAsyncInsertQuery');

SET async_insert = 1;
INSERT INTO t_async_insert_02474_1 VALUES (); -- { serverError SYNTAX_ERROR }
INSERT INTO t_async_insert_02474_1 VALUES ([1,2,3], 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
INSERT INTO t_async_insert_02474_1 format JSONEachRow {"id" : 1} {"x"} -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
INSERT INTO t_async_insert_02474_1 Values (throwIf(4),''); -- { serverError 395 }

SYSTEM FLUSH LOGS;


SELECT current_value - previous_value
FROM (
    SELECT event, value as current_value FROM system.events WHERE event like 'FailedAsyncInsertQuery'
) AS previous
ALL LEFT JOIN (
    SELECT event, value as previous_value FROM current_failed_async_query_metrics
) AS current
on previous.event = current.event;

DROP TABLE IF EXISTS current_failed_async_query_metrics;
DROP TABLE IF EXISTS t_async_insert_02474_1;