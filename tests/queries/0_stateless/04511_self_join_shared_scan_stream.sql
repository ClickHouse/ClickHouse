-- Tags: no-parallel-replicas, no-old-analyzer, no-darwin
-- no-old-analyzer: the optimization requires the analyzer.
-- no-darwin: STREAM reads are Linux-only.

-- A `STREAM` scan is unbounded and keeps producing newly committed rows, so the
-- self-join shared-scan rewrite must not buffer it or replay it from a one-shot
-- buffer. Every mix of `STREAM` and plain scans must keep two `ReadFromMergeTree`.

SET enable_analyzer = 1; -- the rewrite requires the analyzer
SET enable_streaming_queries = 1; -- STREAM scans require it
SET query_plan_optimize_self_join_shared_scan = 1; -- the setting under test
SET enable_join_runtime_filters = 0; -- a runtime filter makes the scan non-plain and blocks the rewrite
SET enable_parallel_replicas = 0; -- reading with parallel replicas blocks the rewrite
SET enable_shared_storage_snapshot_in_query = 1; -- the rewrite requires both scans to share one storage snapshot

DROP TABLE IF EXISTS t_sjss_stream;
CREATE TABLE t_sjss_stream (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_stream SELECT number, toString(number) FROM numbers(10);

-- STREAM on the probe (left) side.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_stream AS a STREAM INNER JOIN t_sjss_stream AS b ON a.x = b.x
);

-- STREAM on the build (right) side.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_stream AS a INNER JOIN t_sjss_stream AS b STREAM ON a.x = b.x
);

-- STREAM on both sides.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_stream AS a STREAM INNER JOIN t_sjss_stream AS b STREAM ON a.x = b.x
);

DROP TABLE t_sjss_stream;
