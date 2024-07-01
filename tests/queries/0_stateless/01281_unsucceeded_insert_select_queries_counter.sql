-- Tags: no-parallel, no-fasttest

DROP TABLE IF EXISTS current_failed_query_metrics;
DROP TABLE IF EXISTS to_insert;

CREATE TABLE current_failed_query_metrics (event LowCardinality(String), value UInt64) ENGINE = Memory();


INSERT INTO current_failed_query_metrics 
SELECT event, value
FROM system.events
WHERE event in ('FailedQuery', 'FailedInsertQuery', 'FailedSelectQuery');

CREATE TABLE to_insert (value UInt64) ENGINE = Memory();

-- Failed insert before execution
INSERT INTO table_that_do_not_exists VALUES (42); -- { serverError UNKNOWN_TABLE }

SELECT current_value - previous_value
FROM (
    SELECT event, value as current_value FROM system.events WHERE event like 'FailedInsertQuery'
) AS previous
ALL LEFT JOIN (
    SELECT event, value as previous_value FROM current_failed_query_metrics
) AS current
on previous.event = current.event;


-- Failed insert in execution
INSERT INTO to_insert SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT current_value - previous_value
FROM (
    SELECT event, value as current_value FROM system.events WHERE event like 'FailedInsertQuery'
) AS previous
ALL LEFT JOIN (
    SELECT event, value as previous_value FROM current_failed_query_metrics
) AS current
on previous.event = current.event;


-- Failed select before execution
SELECT * FROM table_that_do_not_exists; -- { serverError UNKNOWN_TABLE }

SELECT current_value - previous_value
FROM (
    SELECT event, value as current_value FROM system.events WHERE event like 'FailedSelectQuery'
) AS previous
ALL LEFT  JOIN (
    SELECT event, value as previous_value FROM current_failed_query_metrics
) AS current
on previous.event = current.event;

-- Failed select in execution
SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT current_value - previous_value
FROM (
    SELECT event, value as current_value FROM system.events WHERE event like 'FailedSelectQuery'
) AS previous
ALL LEFT JOIN (
    SELECT event, value as previous_value FROM current_failed_query_metrics
) AS current
on previous.event = current.event;


DROP TABLE current_failed_query_metrics;
DROP TABLE to_insert;
