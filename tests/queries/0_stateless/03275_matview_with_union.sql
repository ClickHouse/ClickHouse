DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS matview;

SET use_async_executor_for_materialized_views=1;

CREATE TABLE src (
    event_time DateTime,
    key UInt64,
    value Int64
)
ENGINE = MergeTree()
ORDER BY (event_time, key);

CREATE TABLE dst (
    step UInt16,
    rounded_event_time DateTime,
    key UInt64,
    value AggregateFunction(max, Int64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (step, rounded_event_time, key);

CREATE MATERIALIZED VIEW matview TO dst
(
    step UInt16,
    rounded_event_time DateTime,
    key UInt64,
    value AggregateFunction(max, Int64)
) AS
SELECT * FROM (
    SELECT
        1 AS step,
        key,
        intDiv(toUnixTimestamp(event_time), step) * step AS rounded_event_time,
        initializeAggregation('maxState', value) AS value
    FROM src
    ORDER BY
        rounded_event_time,
        key
    UNION ALL
    SELECT
        5 AS step,
        key,
        intDiv(toUnixTimestamp(event_time), step) * step AS rounded_event_time,
        initializeAggregation('maxState', value) AS value
    FROM src
    ORDER BY
        rounded_event_time,
        key
    UNION ALL
    SELECT
        15 AS step,
        key,
        intDiv(toUnixTimestamp(event_time), step) * step AS rounded_event_time,
        initializeAggregation('maxState', value) AS value
    FROM src
    ORDER BY
        rounded_event_time,
        key
    UNION ALL
    SELECT
        30 AS step,
        key,
        intDiv(toUnixTimestamp(event_time), step) * step AS rounded_event_time,
        initializeAggregation('maxState', value) AS value
    FROM src
    ORDER BY
        rounded_event_time,
        key
)
ORDER BY step, rounded_event_time, key SETTINGS query_plan_remove_redundant_sorting = 0;

set optimize_on_insert = 1;

INSERT INTO src SELECT toDateTime('2020-10-01 00:00:00') + number, number % 100, number from numbers(1000);

SELECT count() FROM dst;

SELECT count(), key FROM dst WHERE step = 30 group by key ORDER BY key LIMIT 5;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS matview;
