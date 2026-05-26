-- Tags: no-parallel-replicas
-- Optimization doesn't work with parallel replicas

-- Mirrors the production schema: ORDER BY (MetricName, TimeUnix) with Map partition
-- columns, matching the query pattern from Nutanix issue:
--   SELECT sum(prev_count) FROM (
--       SELECT lagInFrame(Count) OVER (
--           PARTITION BY MetricName, Attributes ORDER BY TimeUnix
--       ) AS prev_count FROM t)
CREATE TABLE lag_streaming_t (
    MetricName LowCardinality(String),
    TimeUnix UInt64,
    Count UInt64,
    Attributes Map(LowCardinality(String), String)
) ENGINE = MergeTree()
ORDER BY (MetricName, TimeUnix)
SETTINGS index_granularity = 8192;

INSERT INTO lag_streaming_t
SELECT
    concat('metric_', toString(number % 10)) AS MetricName,
    number * 1000 AS TimeUnix,
    number AS Count,
    map('k1', toString(number % 5)) AS Attributes
FROM numbers(0, 100000);

SET max_threads = 4, optimize_read_in_order = 1;

-- Without the optimization, no StreamingLag in the pipeline.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0
);

-- With the optimization, StreamingLagTransform replaces FinishSortingTransform + WindowTransform.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Three-argument form lagInFrame(col, 1, default) must also activate streaming.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count, 1, toUInt64(0)) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Non-1 offset must NOT activate streaming.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count, 2) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Verify correctness: results are identical with and without the optimization.
SELECT
    (
        SELECT sum(prev_count) FROM (
            SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
            FROM lag_streaming_t
            SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0
        )
    ) AS without_opt,
    (
        SELECT sum(prev_count) FROM (
            SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
            FROM lag_streaming_t
            SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
        )
    ) AS with_opt,
    without_opt = with_opt AS correct;

DROP TABLE lag_streaming_t;
