-- Tags: no-parallel-replicas
-- Optimization doesn't work with parallel replicas

-- A window without an ORDER BY must NOT activate StreamingLag: the regular window path
-- sorts by the full PARTITION BY, while the streaming path would merge by the storage-key
-- prefix alone and visit the rows of each suffix partition in storage-key order instead,
-- changing which row lagInFrame considers "previous".
CREATE TABLE lag_streaming_no_order_by_t (
    MetricName LowCardinality(String),
    TimeUnix UInt64,
    Count UInt64,
    Attributes Map(LowCardinality(String), String)
) ENGINE = MergeTree()
ORDER BY (MetricName, TimeUnix)
SETTINGS index_granularity = 8192;

INSERT INTO lag_streaming_no_order_by_t
SELECT
    concat('metric_', toString(number % 10)) AS MetricName,
    number * 1000 AS TimeUnix,
    number AS Count,
    map('k1', toString(intDiv(number, 10) % 5)) AS Attributes
FROM numbers(1, 100000);

-- Note: the rewrite is a query-plan optimization driven by the top-level query context, so a
-- `SETTINGS` clause inside a subquery does not switch it on; correctness checks use session-level `SET`.
SET max_threads = 4, optimize_read_in_order = 1;

-- Must NOT activate streaming: no window ORDER BY.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes) AS prev_count
    FROM lag_streaming_no_order_by_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Order-independent correctness invariant: within every partition exactly one row (the
-- first in whatever order the partition is traversed) gets the default value 0; all Count
-- values are >= 1, so a zero can only be the default.
SET query_plan_reuse_storage_ordering_for_window_functions = 1;
SELECT countIf(prev_count = 0), count()
FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes) AS prev_count
    FROM lag_streaming_no_order_by_t
);

DROP TABLE lag_streaming_no_order_by_t;
