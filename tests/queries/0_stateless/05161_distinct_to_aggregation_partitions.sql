-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ

SET query_plan_convert_distinct_to_aggregation = 1;
SET allow_distinct_partitions_independently = 1;
SET max_threads = 8;
-- The per-partition optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;

-- Independent per-partition DISTINCT is not applied when a DISTINCT size limit is set, and some CI
-- configurations set these limits at the server level, so pin them to unlimited.
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;

-- The pretty EXPLAIN output decorates plan lines with tree-drawing characters; use the legacy format
-- so the assertions below match plain step lines.
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_distinct_partitions;
CREATE TABLE t_distinct_partitions (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES t_distinct_partitions;
INSERT INTO t_distinct_partitions SELECT number % 1000, number FROM numbers_mt(1e4);

-- The partition key is a function of the `DISTINCT` key, so the final `DISTINCT` deduplicates each
-- partition's stream on its own and never merges the streams. It is already parallel and keeps its transform.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM t_distinct_partitions) WHERE explain LIKE '%Distinct%' OR explain LIKE '%Skip stream merging%' OR explain LIKE '%Aggregating%';
SELECT count(), sum(a) FROM (SELECT DISTINCT a FROM t_distinct_partitions);

-- Without the per-partition optimization the final `DISTINCT` merges its streams, and aggregation replaces it.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM t_distinct_partitions SETTINGS allow_distinct_partitions_independently = 0) WHERE explain LIKE '%Distinct%' OR explain LIKE '%Skip stream merging%' OR explain LIKE '%Aggregating%';
SELECT count(), sum(a) FROM (SELECT DISTINCT a FROM t_distinct_partitions SETTINGS allow_distinct_partitions_independently = 0);

-- A `DISTINCT` size limit keeps the streams merged so that the limit stays global. Aggregation replaces
-- the merge and enforces the same limit.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM t_distinct_partitions SETTINGS max_rows_in_distinct = 100000) WHERE explain LIKE '%Distinct%' OR explain LIKE '%Skip stream merging%' OR explain LIKE '%Aggregating%';
SELECT count(), sum(a) FROM (SELECT DISTINCT a FROM t_distinct_partitions SETTINGS max_rows_in_distinct = 100000);

-- Keys that do not determine the partition need the merge, so aggregation replaces it.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT b FROM t_distinct_partitions) WHERE explain LIKE '%Distinct%' OR explain LIKE '%Skip stream merging%' OR explain LIKE '%Aggregating%';
SELECT count(), sum(b) FROM (SELECT DISTINCT b FROM t_distinct_partitions);

DROP TABLE t_distinct_partitions;
