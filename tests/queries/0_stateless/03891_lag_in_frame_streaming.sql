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

-- This test pins the obsolete planner to keep covering the legacy `optimize_read_in_order`
-- helper (`tryReuseStorageOrderingForWindowFunctions`).  The default planner path is covered by
-- `05037_lag_in_frame_streaming_default_planner`.
-- Note: the rewrite is a query-plan optimization driven by the top-level query context, so a
-- `SETTINGS` clause inside a subquery does not switch it on; correctness checks use session-level `SET`.
SET max_threads = 4, optimize_read_in_order = 1, query_plan_read_in_order = 0, allow_experimental_analyzer = 0;

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
SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SELECT sum(prev_count) FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix DESC) AS prev_count
    FROM lag_streaming_desc_t
);
SET query_plan_reuse_storage_ordering_for_window_functions = 1;
SELECT sum(prev_count) FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix DESC) AS prev_count
    FROM lag_streaming_desc_t
);

DROP TABLE lag_streaming_desc_t;

-- Verify correctness: results are identical with and without the optimization.
SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SELECT sum(prev_count) FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_t
);
SET query_plan_reuse_storage_ordering_for_window_functions = 1;
SELECT sum(prev_count) FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_t
);

DROP TABLE lag_streaming_t;

-- Floating-point partition keys: the streaming path matches partition keys with a SipHash of
-- their raw bytes (`updateHashWithValue`), whereas `WindowTransform` uses `compareAt`.  These
-- disagree for floats -- `compareAt` treats `-0.0` and `+0.0` (and all `NaN`s) as equal, the raw
-- hash does not -- so a float-bearing partition key would be split into several partitions and
-- produce wrong `lagInFrame` results.  The optimization must therefore NOT activate on such keys
-- (lifting this requires the canonicalization tracked in issue #105941).
CREATE TABLE lag_streaming_float_t (
    MetricName LowCardinality(String),
    TimeUnix UInt64,
    Count UInt64,
    FloatAttr Float64,
    NullableFloatAttr Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY (MetricName, TimeUnix)
SETTINGS index_granularity = 8192;

-- FloatAttr alternates +0.0 / -0.0 within a single prefix group: `compareAt` sees one partition,
-- a raw-byte hash would split it -- exactly the case that would corrupt results without the gate.
INSERT INTO lag_streaming_float_t
SELECT
    'm' AS MetricName,
    number AS TimeUnix,
    number AS Count,
    if(number % 2 = 0, toFloat64(0.0), toFloat64(-0.0)) AS FloatAttr,
    if(number % 2 = 0, toFloat64(0.0), toFloat64(-0.0)) AS NullableFloatAttr
FROM numbers(0, 1000);

-- Float64 suffix partition key: streaming must NOT activate.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, FloatAttr ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_float_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Nullable(Float64) suffix partition key (float nested inside Nullable): streaming must NOT activate.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, NullableFloatAttr ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_float_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- With the setting enabled the float key falls back to `WindowTransform`, so the result matches
-- the unoptimized path (and is not corrupted by signed-zero partition splitting).
SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SELECT sum(prev_count) FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, FloatAttr ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_float_t
);
SET query_plan_reuse_storage_ordering_for_window_functions = 1;
SELECT sum(prev_count) FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, FloatAttr ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_float_t
);

DROP TABLE lag_streaming_float_t;

-- Runtime-typed partition keys: a `Dynamic` (or `JSON`/`Object`, or `Variant` holding one of those)
-- doesn't reveal its actual per-row type via `forEachChild` (which only walks statically-declared
-- child types), so the float-only check above cannot see through it.  But the same disagreement
-- between the raw-byte hash (`updateHashWithValue`) and `compareAt` still applies once a `Dynamic`
-- value happens to hold a float: `ColumnDynamic::compareAt` compares the nested `Float64`, while
-- `updateHashWithValue` hashes the variant discriminator plus the raw float bits.  The optimization
-- must therefore also reject partition keys with a dynamic internal structure
-- (`IDataType::hasDynamicStructure`).
SET allow_suspicious_types_in_group_by = 1;

CREATE TABLE lag_streaming_dynamic_t (
    MetricName LowCardinality(String),
    TimeUnix UInt64,
    Count UInt64,
    DynAttr Dynamic
) ENGINE = MergeTree()
ORDER BY (MetricName, TimeUnix)
SETTINGS index_granularity = 8192;

-- DynAttr alternates +0.0 / -0.0 (stored as Float64 inside Dynamic) within a single prefix group.
INSERT INTO lag_streaming_dynamic_t
SELECT
    'm' AS MetricName,
    number AS TimeUnix,
    number AS Count,
    if(number % 2 = 0, toFloat64(0.0), toFloat64(-0.0)) AS DynAttr
FROM numbers(0, 1000);

-- Dynamic suffix partition key: streaming must NOT activate.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, DynAttr ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_dynamic_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- With the setting enabled the Dynamic key falls back to `WindowTransform`, so the result matches
-- the unoptimized path (and is not corrupted by signed-zero partition splitting).
SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SELECT sum(prev_count) FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, DynAttr ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_dynamic_t
);
SET query_plan_reuse_storage_ordering_for_window_functions = 1;
SELECT sum(prev_count) FROM (
    SELECT lagInFrame(Count) OVER (PARTITION BY MetricName, DynAttr ORDER BY TimeUnix) AS prev_count
    FROM lag_streaming_dynamic_t
);

DROP TABLE lag_streaming_dynamic_t;

-- Stacked windows: converting the sort below the first window to `MergeOnly` weakens the
-- delivered order from the window's `full_sort_description` (`partition_by + order_by`) to only
-- `prefix + order_by`.  Plan construction decides whether a later window needs its own sort
-- *before* this optimization runs, by checking prefixes of the previous window's original
-- `full_sort_description`: here `row_number() OVER (PARTITION BY MetricName, Attributes)` is
-- planned without a sort because `(MetricName, Attributes)` is a prefix of
-- `(MetricName, Attributes, TimeUnix)`.  Streaming the first window would feed the second one
-- data ordered only by `(MetricName, TimeUnix)`, interleaving its partitions.  The optimization
-- must therefore NOT activate when a `WindowStep` sits above the candidate without a full sort
-- in between.
CREATE TABLE lag_streaming_stacked_t (
    MetricName LowCardinality(String),
    TimeUnix UInt64,
    Count UInt64,
    Attributes Map(LowCardinality(String), String)
) ENGINE = MergeTree()
ORDER BY (MetricName, TimeUnix)
SETTINGS index_granularity = 8192;

-- `intDiv(number, 10) % 5` makes `Attributes` alternate between consecutive rows of the same
-- `MetricName` (it must not be a function of `MetricName`, or the interleaving below would be
-- unobservable): under the weakened `(MetricName, TimeUnix)` order the `(MetricName, Attributes)`
-- partitions of the second window are maximally interleaved.
INSERT INTO lag_streaming_stacked_t
SELECT
    concat('metric_', toString(number % 10)) AS MetricName,
    number * 1000 AS TimeUnix,
    number AS Count,
    map('k1', toString(intDiv(number, 10) % 5)) AS Attributes
FROM numbers(0, 100000);

-- A second window reusing the first window's original sort order: streaming must NOT activate.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT
        lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count,
        row_number() OVER (PARTITION BY MetricName, Attributes) AS rn
    FROM lag_streaming_stacked_t
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

-- Correctness: the second window's result must match the unoptimized path.  Both window results
-- must be consumed, otherwise the unused `lagInFrame` window is removed from the plan and the
-- streaming rewrite (whose misapplication this checks) never happens.
SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SELECT (sum(rn), sum(prev_count)) FROM (
    SELECT
        lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count,
        row_number() OVER (PARTITION BY MetricName, Attributes) AS rn
    FROM lag_streaming_stacked_t
);
SET query_plan_reuse_storage_ordering_for_window_functions = 1;
SELECT (sum(rn), sum(prev_count)) FROM (
    SELECT
        lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count,
        row_number() OVER (PARTITION BY MetricName, Attributes) AS rn
    FROM lag_streaming_stacked_t
);

-- A later window whose sort keys are not covered by the first window's order gets its own
-- full `SortingStep`, which re-establishes order independently of its input: the first
-- window may still stream.
SELECT countIf(explain LIKE '%StreamingLag%')
FROM (
    EXPLAIN pipeline
    SELECT row_number() OVER (PARTITION BY MetricName ORDER BY prev_count) AS rn
    FROM (
        SELECT MetricName, lagInFrame(Count) OVER (PARTITION BY MetricName, Attributes ORDER BY TimeUnix) AS prev_count
        FROM lag_streaming_stacked_t
    )
    SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1
);

DROP TABLE lag_streaming_stacked_t;
