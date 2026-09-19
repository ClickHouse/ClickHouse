-- Tags: no-parallel-replicas
-- Optimization doesn't work with parallel replicas

-- Covers the shipped path: the analyzer together with the query-plan `optimizeReadInOrder` pass,
-- where the sort description holds analyzer-qualified column names (`03891_lag_in_frame_streaming`
-- holds the broader eligibility matrix).

CREATE TABLE lag_streaming_default_t (
    MetricName LowCardinality(String),
    TimeUnix UInt64,
    Count UInt64,
    Attributes Map(LowCardinality(String), String)
) ENGINE = MergeTree()
ORDER BY (MetricName, TimeUnix)
SETTINGS index_granularity = 8192;

INSERT INTO lag_streaming_default_t
SELECT
    concat('metric_', toString(number % 10)) AS MetricName,
    number * 1000 AS TimeUnix,
    number AS Count,
    map('k1', toString(number % 5)) AS Attributes
FROM numbers(0, 100000);

-- Note: the rewrite is a query-plan optimization driven by the top-level query context, so a
-- `SETTINGS` clause inside a subquery does not switch it on; correctness checks use session-level `SET`.
SET max_threads = 4, optimize_read_in_order = 1;

-- Without the optimization, no StreamingLag in the pipeline.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_default_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0
);

-- With the optimization, `StreamingLagTransform` replaces `FinishSortingTransform` + `WindowTransform`.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_default_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- The window ORDER BY column may be an alias of the storage key column.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY t) AS prev_count
    FROM (SELECT MetricName, Attributes, Count, TimeUnix AS t FROM lag_streaming_default_t)
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Non-1 offset must NOT activate streaming.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count, 2) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_default_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- ORDER BY TimeUnix DESC mismatches the storage ASC key: must NOT activate streaming.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix DESC) AS prev_count
    FROM lag_streaming_default_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Correctness: results are identical with and without the optimization.
SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SELECT sum(prev_count) FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_default_t
);
SET query_plan_reuse_storage_ordering_for_window_functions = 1;
SELECT sum(prev_count) FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_default_t
);

-- The rewrite widens the read-in-order prefix by re-requesting `requestReadingInOrder` and must
-- carry over the read limit installed by the earlier request.  The streaming pipeline has no
-- blocking sort, so a query with a `LIMIT` stops the in-order scan after the first granules
-- instead of reading the whole table the way the `WindowTransform` path does.
SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
FROM lag_streaming_default_t
LIMIT 10
SETTINGS max_threads = 1, query_plan_reuse_storage_ordering_for_window_functions = 1, log_comment = '05037_lag_streaming_limit'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- 100000 rows in the table; `LIMIT 10` needs a handful of granules, not a full scan.
SELECT read_rows < 100000
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05037_lag_streaming_limit' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE lag_streaming_default_t;
