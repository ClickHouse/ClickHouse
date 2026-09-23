-- Tags: no-parallel-replicas
-- The streaming `lagInFrame` rewrite does not work with parallel replicas.

-- `query_plan_reuse_storage_ordering_for_window_functions` rewrites `WindowStep` + `FinishSorting`
-- into a `MergeOnly` sort under a streaming `WindowStep`. Neither is serializable, so the rewrite
-- must not run while the plan may be shipped out (`make_distributed_plan`, `serialize_query_plan`):
-- an eligible query has to keep the plain `WindowTransform` path there instead of failing with
-- `SUPPORT_IS_DISABLED` when the plan is converted to fragments.

DROP TABLE IF EXISTS lag_streaming_distributed_t;

CREATE TABLE lag_streaming_distributed_t (
    MetricName LowCardinality(String),
    TimeUnix UInt64,
    Count UInt64,
    Attributes Map(LowCardinality(String), String)
) ENGINE = MergeTree()
ORDER BY (MetricName, TimeUnix)
SETTINGS index_granularity = 8192;

INSERT INTO lag_streaming_distributed_t
SELECT
    concat('metric_', toString(number % 10)) AS MetricName,
    number * 1000 AS TimeUnix,
    number AS Count,
    map('k1', toString(number % 5)) AS Attributes
FROM numbers(0, 100000);

-- `max_rows_to_group_by` and `extremes` are pinned because the test runner may randomize them and
-- `make_distributed_plan` refuses to distribute such a query (the no-fallback case below then throws).
SET max_threads = 4, optimize_read_in_order = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0, extremes = 0;

-- Control: the query is eligible for the rewrite on a plain single-node plan.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_distributed_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Under a distributed plan the rewrite must not fire.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_distributed_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1,
        make_distributed_plan = 1, distributed_plan_execute_locally = 1
);

-- The query still executes and returns the same result with the setting on and off, both when
-- the plan is distributed and when plan serialization is requested. The rewrite is a query-plan
-- optimization driven by the top-level query context, so these use session-level `SET`.
SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SELECT 'distributed', sum(prev_count), count()
FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_distributed_t
)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SET query_plan_reuse_storage_ordering_for_window_functions = 1;
SELECT 'distributed', sum(prev_count), count()
FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_distributed_t
)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT 'distributed, no fallback', sum(prev_count), count()
FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_distributed_t
)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0;

SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SELECT 'serialized', sum(prev_count), count()
FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_distributed_t
)
SETTINGS serialize_query_plan = 1;

SET query_plan_reuse_storage_ordering_for_window_functions = 1;
SELECT 'serialized', sum(prev_count), count()
FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_distributed_t
)
SETTINGS serialize_query_plan = 1;

DROP TABLE lag_streaming_distributed_t;
