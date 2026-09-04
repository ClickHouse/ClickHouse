-- Tags: long, no-msan, no-random-settings, no-random-merge-tree-settings
-- no-msan: 16 scenarios with INSERT + EXPLAIN; the MSan slowdown blows past the test timeout
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ

-- max_threads = 8: the cost heuristic requires partitions >= max_threads/2, and every positive case
-- below has 8 balanced partitions.
SET max_threads = 8;
-- The optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;

SET max_rows_to_sort = 0;
SET max_bytes_to_sort = 0;

-- The pretty EXPLAIN output decorates plan lines with tree-drawing characters; use the legacy format
-- so the assertions below match plain `Skip scatter by partition: 1` lines.
SET explain_query_plan_default = 'legacy';

-- { echo }

-- partition key equals the window PARTITION BY column
DROP TABLE IF EXISTS test_win_partition_eq_key;
CREATE TABLE test_win_partition_eq_key (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_win_partition_eq_key;
INSERT INTO test_win_partition_eq_key SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO test_win_partition_eq_key SELECT number % 64, number + 400 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_partition_eq_key SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT sum(cityHash64(a, s)) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_partition_eq_key) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(a, s)) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_partition_eq_key) SETTINGS allow_window_partitions_independently = 1);
-- the scatter disappears from the pipeline and every partition stream gets its own window transform
SELECT count() FROM (EXPLAIN PIPELINE SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_partition_eq_key SETTINGS allow_window_partitions_independently = 1) WHERE explain ILIKE '%ScatterByPartitionTransform%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN PIPELINE SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_partition_eq_key SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%WindowTransform%';
-- NEGATIVE: optimization disabled keeps the scatter
SELECT count() FROM (EXPLAIN PIPELINE SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_partition_eq_key SETTINGS allow_window_partitions_independently = 0) WHERE explain ILIKE '%ScatterByPartitionTransform%';
DROP TABLE test_win_partition_eq_key;

-- partition key is a function of the window PARTITION BY column; WHERE clause in the chain
DROP TABLE IF EXISTS test_win_partition_func_of_key;
CREATE TABLE test_win_partition_func_of_key (d Date, x UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY toYYYYMM(d);
SYSTEM STOP MERGES test_win_partition_func_of_key;
INSERT INTO test_win_partition_func_of_key SELECT toDate('2024-01-01') + (number % 240), number FROM numbers_mt(480);
INSERT INTO test_win_partition_func_of_key SELECT toDate('2024-01-01') + (number % 240), number + 480 FROM numbers_mt(480);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT d, sum(x) OVER (PARTITION BY d ORDER BY x) FROM test_win_partition_func_of_key WHERE x > 0 SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT sum(cityHash64(d, s)) FROM (SELECT d, sum(x) OVER (PARTITION BY d ORDER BY x) AS s FROM test_win_partition_func_of_key WHERE x > 0) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(d, s)) FROM (SELECT d, sum(x) OVER (PARTITION BY d ORDER BY x) AS s FROM test_win_partition_func_of_key WHERE x > 0) SETTINGS allow_window_partitions_independently = 1);
DROP TABLE test_win_partition_func_of_key;

-- window PARTITION BY is a superset of the partition columns
DROP TABLE IF EXISTS test_win_key_superset;
CREATE TABLE test_win_key_superset (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_win_key_superset SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a, b % 10) FROM test_win_key_superset SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT sum(cityHash64(a, s)) FROM (SELECT a, sum(b) OVER (PARTITION BY a, b % 10) AS s FROM test_win_key_superset) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(a, s)) FROM (SELECT a, sum(b) OVER (PARTITION BY a, b % 10) AS s FROM test_win_key_superset) SETTINGS allow_window_partitions_independently = 1);
DROP TABLE test_win_key_superset;

-- stacked windows: the sorting of the upper window skips the scatter through disjointness propagation
-- (the lower window keeps rows in their streams, so the property reaches the upper sorting)
DROP TABLE IF EXISTS test_win_stacked;
CREATE TABLE test_win_stacked (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_win_stacked;
INSERT INTO test_win_stacked SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO test_win_stacked SELECT number % 64, number + 400 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b), avg(b) OVER (PARTITION BY a ORDER BY b DESC) FROM test_win_stacked SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT sum(cityHash64(a, s1, s2)) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s1, avg(b) OVER (PARTITION BY a ORDER BY b DESC) AS s2 FROM test_win_stacked) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(a, s1, s2)) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s1, avg(b) OVER (PARTITION BY a ORDER BY b DESC) AS s2 FROM test_win_stacked) SETTINGS allow_window_partitions_independently = 1);
DROP TABLE test_win_stacked;

-- A plain OVER () window merges the pipeline to a single stream, so the disjointness does not
-- propagate above it: the sorting below it still skips its scatter, while the sorting above it keeps
-- the scatter that makes the pipeline parallel again.
DROP TABLE IF EXISTS test_win_global_between;
CREATE TABLE test_win_global_between (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_win_global_between;
INSERT INTO test_win_global_between SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO test_win_global_between SELECT number % 64, number + 400 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT s1, g, sum(b) OVER (PARTITION BY a ORDER BY b DESC) AS s2 FROM (SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b) AS s1, sum(b) OVER () AS g FROM test_win_global_between) SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
-- the scatter above the single-stream window survives in the pipeline and re-widens it
SELECT count() FROM (EXPLAIN PIPELINE SELECT s1, g, sum(b) OVER (PARTITION BY a ORDER BY b DESC) AS s2 FROM (SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b) AS s1, sum(b) OVER () AS g FROM test_win_global_between) SETTINGS allow_window_partitions_independently = 1) WHERE explain ILIKE '%ScatterByPartitionTransform%';
SELECT (SELECT sum(cityHash64(s1, g, s2)) FROM (SELECT s1, g, sum(b) OVER (PARTITION BY a ORDER BY b DESC) AS s2 FROM (SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b) AS s1, sum(b) OVER () AS g FROM test_win_global_between)) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(s1, g, s2)) FROM (SELECT s1, g, sum(b) OVER (PARTITION BY a ORDER BY b DESC) AS s2 FROM (SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b) AS s1, sum(b) OVER () AS g FROM test_win_global_between)) SETTINGS allow_window_partitions_independently = 1);
DROP TABLE test_win_global_between;

-- GROUP BY above the window skips merging when the property survives the window; the resize back to
-- max_threads after the last window would mix the streams, so it is disabled for this query
DROP TABLE IF EXISTS test_win_then_group_by;
CREATE TABLE test_win_then_group_by (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_win_then_group_by SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, max(s) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_then_group_by) GROUP BY a SETTINGS allow_window_partitions_independently = 1, allow_aggregate_partitions_independently = 1, max_rows_to_group_by = 0, query_plan_enable_multithreading_after_window_functions = 0) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%' OR explain LIKE '%Skip merging: 1%';
SELECT (SELECT sum(cityHash64(a, m)) FROM (SELECT a, max(s) AS m FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_then_group_by) GROUP BY a) SETTINGS allow_window_partitions_independently = 0, allow_aggregate_partitions_independently = 0) = (SELECT sum(cityHash64(a, m)) FROM (SELECT a, max(s) AS m FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_then_group_by) GROUP BY a) SETTINGS allow_window_partitions_independently = 1, allow_aggregate_partitions_independently = 1, max_rows_to_group_by = 0, query_plan_enable_multithreading_after_window_functions = 0);
DROP TABLE test_win_then_group_by;

-- NEGATIVE: the window PARTITION BY does not determine the partition
DROP TABLE IF EXISTS test_win_key_not_partition;
CREATE TABLE test_win_key_not_partition (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_win_key_not_partition SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT b, sum(a) OVER (PARTITION BY b % 10) FROM test_win_key_not_partition SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_win_key_not_partition;

-- NEGATIVE: window without PARTITION BY has no scatter to skip
DROP TABLE IF EXISTS test_win_no_partition_by;
CREATE TABLE test_win_no_partition_by (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_win_no_partition_by SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (ORDER BY b) FROM test_win_no_partition_by SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER () FROM test_win_no_partition_by SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_win_no_partition_by;

-- NEGATIVE: only one partition; force bypasses only the cost heuristic
DROP TABLE IF EXISTS test_win_single_partition;
CREATE TABLE test_win_single_partition (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_win_single_partition;
INSERT INTO test_win_single_partition SELECT 8 * (number % 25), number FROM numbers_mt(200);
INSERT INTO test_win_single_partition SELECT 8 * (number % 25), number + 200 FROM numbers_mt(200);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_single_partition SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_win_single_partition;

-- NEGATIVE: too few partitions for the heuristic; force bypasses it
DROP TABLE IF EXISTS test_win_few_partitions;
CREATE TABLE test_win_few_partitions (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 2;
SYSTEM STOP MERGES test_win_few_partitions;
INSERT INTO test_win_few_partitions SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO test_win_few_partitions SELECT number % 64, number + 400 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_few_partitions SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_few_partitions SETTINGS allow_window_partitions_independently = 1, force_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT sum(cityHash64(a, s)) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_few_partitions) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(a, s)) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_few_partitions) SETTINGS allow_window_partitions_independently = 1, force_window_partitions_independently = 1);
DROP TABLE test_win_few_partitions;

-- NEGATIVE: heavily skewed partitions (heuristic declines)
DROP TABLE IF EXISTS test_win_skewed_partitions;
CREATE TABLE test_win_skewed_partitions (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY if(a % 10 < 9, 0, 1 + a % 15);
INSERT INTO test_win_skewed_partitions SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_skewed_partitions SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_win_skewed_partitions;

-- NEGATIVE: FINAL
DROP TABLE IF EXISTS test_win_with_final;
CREATE TABLE test_win_with_final (a UInt32, b UInt32, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY (a, b) PARTITION BY a % 8;
INSERT INTO test_win_with_final SELECT number % 64, number, 1 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_with_final FINAL SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_win_with_final;

-- NEGATIVE: max_rows_to_sort / max_bytes_to_sort are enforced per sort stream, so which rows land in
-- which stream is user-visible; skipping the scatter would regroup the streams by table partition and
-- could fail a query that passes with the scatter. The scatter is kept and per-partition reading is
-- not requested. The propagated skip is declined the same way while the feature that produced the
-- per-partition streams (here LIMIT BY) stays engaged.
DROP TABLE IF EXISTS test_win_sort_limits;
CREATE TABLE test_win_sort_limits (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_win_sort_limits;
INSERT INTO test_win_sort_limits SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO test_win_sort_limits SELECT number % 64, number + 400 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_sort_limits SETTINGS allow_window_partitions_independently = 1, force_window_partitions_independently = 1, max_rows_to_sort = 1000000) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_sort_limits SETTINGS allow_window_partitions_independently = 1, force_window_partitions_independently = 1, max_bytes_to_sort = 100000000) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM (SELECT a, b FROM test_win_sort_limits LIMIT 100 BY a) SETTINGS allow_limit_by_partitions_independently = 1, allow_window_partitions_independently = 1, force_window_partitions_independently = 1, max_rows_to_sort = 1000000) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_win_sort_limits;

-- NEGATIVE: reading in order for the window (the sorting becomes FinishSorting, which merges to a single
-- stream; there is no scatter to skip and per-partition reading is not requested)
DROP TABLE IF EXISTS test_win_read_in_order;
CREATE TABLE test_win_read_in_order (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY a % 8;
INSERT INTO test_win_read_in_order SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM test_win_read_in_order SETTINGS allow_window_partitions_independently = 1, query_plan_reuse_storage_ordering_for_window_functions = 1, optimize_read_in_order = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT sum(cityHash64(a, s)) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_read_in_order) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(a, s)) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_read_in_order) SETTINGS allow_window_partitions_independently = 1, query_plan_reuse_storage_ordering_for_window_functions = 1, optimize_read_in_order = 1);
DROP TABLE test_win_read_in_order;

-- The hash scatter of a window sorting is itself a disjointness source: its output streams are disjoint
-- by the window PARTITION BY columns, with no table PARTITION BY involved.
DROP TABLE IF EXISTS test_win_scatter_source;
CREATE TABLE test_win_scatter_source (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple();
-- several parts so that the read produces multiple streams: with a single stream the pipeline has one
-- thread and no scatter exists at all, making the pipeline assertions below vacuous
SYSTEM STOP MERGES test_win_scatter_source;
INSERT INTO test_win_scatter_source SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO test_win_scatter_source SELECT number % 64, number + 400 FROM numbers_mt(400);
INSERT INTO test_win_scatter_source SELECT number % 64, number + 800 FROM numbers_mt(400);
INSERT INTO test_win_scatter_source SELECT number % 64, number + 1200 FROM numbers_mt(400);
INSERT INTO test_win_scatter_source SELECT number % 64, number + 1600 FROM numbers_mt(400);
INSERT INTO test_win_scatter_source SELECT number % 64, number + 2000 FROM numbers_mt(400);
INSERT INTO test_win_scatter_source SELECT number % 64, number + 2400 FROM numbers_mt(400);
INSERT INTO test_win_scatter_source SELECT number % 64, number + 2800 FROM numbers_mt(400);
-- stacked windows: the first sorting scatters, the second one reuses its distribution and skips its own
-- scatter (one skip marker, no per-partition reading)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b), avg(b) OVER (PARTITION BY a ORDER BY b DESC) FROM test_win_scatter_source SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT count() FROM (EXPLAIN PIPELINE SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b), avg(b) OVER (PARTITION BY a ORDER BY b DESC) FROM test_win_scatter_source SETTINGS allow_window_partitions_independently = 1) WHERE explain ILIKE '%ScatterByPartitionTransform%';
SELECT count() FROM (EXPLAIN PIPELINE SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b), avg(b) OVER (PARTITION BY a ORDER BY b DESC) FROM test_win_scatter_source SETTINGS allow_window_partitions_independently = 0) WHERE explain ILIKE '%ScatterByPartitionTransform%';
SELECT (SELECT sum(cityHash64(a, s1, s2)) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s1, avg(b) OVER (PARTITION BY a ORDER BY b DESC) AS s2 FROM test_win_scatter_source) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(a, s1, s2)) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s1, avg(b) OVER (PARTITION BY a ORDER BY b DESC) AS s2 FROM test_win_scatter_source) SETTINGS allow_window_partitions_independently = 1);
-- GROUP BY above the window skips merging: the aggregation keys determine the scatter columns
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, max(s) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_scatter_source) GROUP BY a SETTINGS allow_aggregate_partitions_independently = 1, max_rows_to_group_by = 0, query_plan_enable_multithreading_after_window_functions = 0) WHERE explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT sum(cityHash64(a, m)) FROM (SELECT a, max(s) AS m FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_scatter_source) GROUP BY a) SETTINGS allow_aggregate_partitions_independently = 0) = (SELECT sum(cityHash64(a, m)) FROM (SELECT a, max(s) AS m FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_scatter_source) GROUP BY a) SETTINGS allow_aggregate_partitions_independently = 1, max_rows_to_group_by = 0, query_plan_enable_multithreading_after_window_functions = 0);
-- final DISTINCT above the window skips the cross-stream merge
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a, s FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_scatter_source) SETTINGS allow_distinct_partitions_independently = 1, max_rows_in_distinct = 0, max_bytes_in_distinct = 0, query_plan_enable_multithreading_after_window_functions = 0) WHERE explain LIKE '%Skip stream merging%';
SELECT (SELECT count() FROM (SELECT DISTINCT a, s FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_scatter_source)) SETTINGS allow_distinct_partitions_independently = 0) = (SELECT count() FROM (SELECT DISTINCT a, s FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_scatter_source)) SETTINGS allow_distinct_partitions_independently = 1, max_rows_in_distinct = 0, max_bytes_in_distinct = 0, query_plan_enable_multithreading_after_window_functions = 0);
-- LIMIT BY above the window skips the cross-stream merge
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, s FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_scatter_source) LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1, query_plan_enable_multithreading_after_window_functions = 0) WHERE explain LIKE '%Skip stream merging%';
SELECT (SELECT count() FROM (SELECT a, s FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_scatter_source) LIMIT 2 BY a) SETTINGS allow_limit_by_partitions_independently = 0) = (SELECT count() FROM (SELECT a, s FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_scatter_source) LIMIT 2 BY a) SETTINGS allow_limit_by_partitions_independently = 1, query_plan_enable_multithreading_after_window_functions = 0);
-- NEGATIVE: the aggregation key does not determine the scatter columns (a does not determine (a, b))
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, max(s) FROM (SELECT a, sum(b) OVER (PARTITION BY a, b ORDER BY b) AS s FROM test_win_scatter_source) GROUP BY a SETTINGS allow_aggregate_partitions_independently = 1, max_rows_to_group_by = 0, query_plan_enable_multithreading_after_window_functions = 0) WHERE explain LIKE '%Skip merging: 1%';
-- NEGATIVE: the resize back to max_threads after the last window mixes the streams
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, max(s) FROM (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) AS s FROM test_win_scatter_source) GROUP BY a SETTINGS allow_aggregate_partitions_independently = 1, max_rows_to_group_by = 0, query_plan_enable_multithreading_after_window_functions = 1) WHERE explain LIKE '%Skip merging: 1%';
DROP TABLE test_win_scatter_source;

-- Disjointness propagated from a per-partition LIMIT BY still passes the window cost heuristic: with
-- fewer partitions than max_threads / 2, skipping the scatter would cap the window processing at the
-- partition count, so the scatter is kept even though the streams are disjoint.
-- force_window_partitions_independently bypasses the heuristic here the same way as for a direct
-- per-partition request.
DROP TABLE IF EXISTS test_win_few_partitions_via_limit_by;
CREATE TABLE test_win_few_partitions_via_limit_by (k UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY k % 2;
SYSTEM STOP MERGES test_win_few_partitions_via_limit_by;
INSERT INTO test_win_few_partitions_via_limit_by SELECT number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() OVER (PARTITION BY k) FROM (SELECT k FROM test_win_few_partitions_via_limit_by LIMIT 1 BY k) SETTINGS allow_window_partitions_independently = 1, allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() OVER (PARTITION BY k) FROM (SELECT k FROM test_win_few_partitions_via_limit_by LIMIT 1 BY k) SETTINGS allow_window_partitions_independently = 1, allow_limit_by_partitions_independently = 1, force_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter by partition%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT sum(cityHash64(c)) FROM (SELECT count() OVER (PARTITION BY k) AS c FROM (SELECT k FROM test_win_few_partitions_via_limit_by LIMIT 1 BY k)) SETTINGS allow_window_partitions_independently = 0, allow_limit_by_partitions_independently = 0) = (SELECT sum(cityHash64(c)) FROM (SELECT count() OVER (PARTITION BY k) AS c FROM (SELECT k FROM test_win_few_partitions_via_limit_by LIMIT 1 BY k)) SETTINGS allow_window_partitions_independently = 1, allow_limit_by_partitions_independently = 1, force_window_partitions_independently = 1);
DROP TABLE test_win_few_partitions_via_limit_by;
