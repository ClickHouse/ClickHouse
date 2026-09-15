-- Explicit `lagInFrame` defaults must be cast exactly like `WindowTransform` does
-- (`WindowFunctionLagLeadImpl::castColumn`: an accurate typed-column cast from the default's
-- type to the value type).  Previously the streaming rewrite converted the raw `Field` with
-- `convertFieldToType`, which loses the source type for nested `Tuple` elements and produced a
-- different default than the normal path (a `Date` day number reinterpreted as `DateTime` seconds).
--
-- Note: the optimization is a query-plan rewrite driven by the top-level query context, so a
-- `SETTINGS` clause inside a subquery does not switch it on; use session-level `SET`.

DROP TABLE IF EXISTS lag_streaming_default_cast_t;

CREATE TABLE lag_streaming_default_cast_t (
    MetricName LowCardinality(String),
    TimeUnix UInt64,
    Ts DateTime('UTC'),
    TsTuple Tuple(DateTime('UTC')),
    Value Int64,
    Attributes Map(LowCardinality(String), String)
) ENGINE = MergeTree()
ORDER BY (MetricName, TimeUnix);

INSERT INTO lag_streaming_default_cast_t
SELECT
    concat('metric_', toString(number % 2)) AS MetricName,
    number * 1000 AS TimeUnix,
    toDateTime('2025-06-01 00:00:00', 'UTC') + number AS Ts,
    tuple(Ts) AS TsTuple,
    100 + number AS Value,
    map('k1', toString(number % 3)) AS Attributes
FROM numbers(0, 1000);

SET max_threads = 4, optimize_read_in_order = 1;

-- The default is only observed on the first row of each partition, so `min` over the whole
-- result is the materialized default.  Each case is computed without and with the optimization
-- and both values must be identical.

SET query_plan_reuse_storage_ordering_for_window_functions = 0;

-- `Date` default for a `DateTime` value: midnight of that day.
SELECT 'datetime', min(prev) FROM (SELECT lagInFrame(Ts, 1, toDate('2024-01-02')) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev FROM lag_streaming_default_cast_t);
-- Nested `Date` inside a `Tuple` default for a `Tuple(DateTime)` value.
SELECT 'tuple', min(prev) FROM (SELECT lagInFrame(TsTuple, 1, tuple(toDate('2024-01-02'))) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev FROM lag_streaming_default_cast_t);
-- Narrower integer default for an `Int64` value.
SELECT 'int', min(prev) FROM (SELECT lagInFrame(Value, 1, toInt8(7)) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev FROM lag_streaming_default_cast_t);

SET query_plan_reuse_storage_ordering_for_window_functions = 1;

-- All three forms activate the streaming transform...
SELECT countIf(explain LIKE '%StreamingLag%') FROM (EXPLAIN pipeline SELECT lagInFrame(Ts, 1, toDate('2024-01-02')) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev FROM lag_streaming_default_cast_t);
SELECT countIf(explain LIKE '%StreamingLag%') FROM (EXPLAIN pipeline SELECT lagInFrame(TsTuple, 1, tuple(toDate('2024-01-02'))) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev FROM lag_streaming_default_cast_t);
SELECT countIf(explain LIKE '%StreamingLag%') FROM (EXPLAIN pipeline SELECT lagInFrame(Value, 1, toInt8(7)) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev FROM lag_streaming_default_cast_t);

-- ...and produce the same defaults as above.
SELECT 'datetime', min(prev) FROM (SELECT lagInFrame(Ts, 1, toDate('2024-01-02')) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev FROM lag_streaming_default_cast_t);
SELECT 'tuple', min(prev) FROM (SELECT lagInFrame(TsTuple, 1, tuple(toDate('2024-01-02'))) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev FROM lag_streaming_default_cast_t);
SELECT 'int', min(prev) FROM (SELECT lagInFrame(Value, 1, toInt8(7)) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev FROM lag_streaming_default_cast_t);

DROP TABLE lag_streaming_default_cast_t;
