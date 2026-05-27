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

-- ORDER BY TimeUnix DESC mismatches the storage ASC key: must NOT activate streaming.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix DESC) AS prev_count
    FROM lag_streaming_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Table with a DESC storage key for TimeUnix.
CREATE TABLE lag_streaming_desc_t (
    MetricName LowCardinality(String),
    TimeUnix UInt64,
    Count UInt64,
    Attributes Map(LowCardinality(String), String)
) ENGINE = MergeTree()
ORDER BY (MetricName, TimeUnix DESC)
SETTINGS index_granularity = 8192, allow_experimental_reverse_key = 1;

INSERT INTO lag_streaming_desc_t
SELECT
    concat('metric_', toString(number % 10)) AS MetricName,
    number * 1000 AS TimeUnix,
    number AS Count,
    map('k1', toString(number % 5)) AS Attributes
FROM numbers(0, 100000);

-- Storage ORDER BY (MetricName, TimeUnix DESC) + window ORDER BY TimeUnix DESC: directions match, must activate streaming.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix DESC) AS prev_count
    FROM lag_streaming_desc_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Same DESC table but window ORDER BY TimeUnix ASC: directions mismatch, must NOT activate streaming.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix ASC) AS prev_count
    FROM lag_streaming_desc_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Verify correctness for DESC storage key.
SELECT
    (
        SELECT sum(prev_count) FROM (
            SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix DESC) AS prev_count
            FROM lag_streaming_desc_t
            SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0
        )
    ) AS without_opt,
    (
        SELECT sum(prev_count) FROM (
            SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix DESC) AS prev_count
            FROM lag_streaming_desc_t
            SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
        )
    ) AS with_opt,
    without_opt = with_opt AS correct;

DROP TABLE lag_streaming_desc_t;

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

-- Float partition key: -0.0 and +0.0 must hash to the same bucket (compareAt treats them equal),
-- and all NaN payloads must hash to the same bucket.
-- Rows ordered by TimeUnix: +0.0 arrives before -0.0, NaN-payload-1 before NaN-payload-2.
-- With canonical hashing there are 2 distinct partition keys, so lagInFrame sees a prior value
-- for rows 2 and 4 → sum = 10 + 30 = 40.  Without canonical hashing all 4 rows start new
-- partitions → sum = NULL ≠ 40, exposing the bug.
CREATE TABLE lag_streaming_float_t (
    MetricName LowCardinality(String),
    FloatKey   Float64,
    TimeUnix   UInt64,
    Count      UInt64
) ENGINE = MergeTree()
ORDER BY (MetricName, TimeUnix)
SETTINGS index_granularity = 8192;

INSERT INTO lag_streaming_float_t VALUES
    ('m1', reinterpretAsFloat64(toUInt64(0x0000000000000000)), 1, 10),   -- +0.0
    ('m1', reinterpretAsFloat64(toUInt64(0x8000000000000000)), 2, 20),   -- -0.0  (same partition as +0.0)
    ('m1', reinterpretAsFloat64(toUInt64(0x7FF0000000000001)), 3, 30),   -- NaN payload 1
    ('m1', reinterpretAsFloat64(toUInt64(0x7FF0000000000002)), 4, 40);   -- NaN payload 2  (same partition as NaN payload 1)

-- Float suffix partition column must activate streaming.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, FloatKey ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_float_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Correctness: canonical hashing must yield the same result as WindowTransform.
SELECT
    (
        SELECT sum(prev_count) FROM (
            SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, FloatKey ORDER BY TimeUnix) AS prev_count
            FROM lag_streaming_float_t
            SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0
        )
    ) AS without_opt,
    (
        SELECT sum(prev_count) FROM (
            SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, FloatKey ORDER BY TimeUnix) AS prev_count
            FROM lag_streaming_float_t
            SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
        )
    ) AS with_opt,
    without_opt = with_opt AS correct;

DROP TABLE lag_streaming_float_t;
