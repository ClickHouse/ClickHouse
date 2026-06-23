-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ

-- DISTINCT-per-partition uses the same cost heuristic as GROUP BY (`isPartitionIndependentProcessingProfitable`).
-- This test exercises every entry point / check of the request method `requestOutputEachPartitionThroughSeparatePortForDistinct`:
--   FINAL (hard incompatibility, not bypassable by force), single partition, too few partitions,
--   too many partitions, and partition skew. Each cost-heuristic check has a `force_distinct_partitions_independently`
--   variant proving force suppresses it. The partition key is always a function of the DISTINCT key, so the
--   only thing under test is the heuristic. Markers: `Skip stream merging` / `Read each partition through separate port`.

SET max_threads = 16; -- cost heuristic threshold: partitions >= max_threads/2 = 8

-- The optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;

-- { echo }

-- POSITIVE baseline: enough balanced partitions (16 >= 8)
DROP TABLE IF EXISTS test_enough_partitions;
CREATE TABLE test_enough_partitions (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 16;
SYSTEM STOP MERGES test_enough_partitions;
INSERT INTO test_enough_partitions SELECT number FROM numbers_mt(1e4);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_enough_partitions SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_enough_partitions SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_enough_partitions SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_enough_partitions;

-- FINAL: hard incompatibility, NOT bypassable by force (rejected before the cost heuristic)
DROP TABLE IF EXISTS test_final;
CREATE TABLE test_final (a UInt32, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY a PARTITION BY a % 16;
INSERT INTO test_final SELECT number, 1 FROM numbers_mt(1e4);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_final FINAL SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_final;

-- single partition: rejected; force bypasses it
DROP TABLE IF EXISTS test_single;
CREATE TABLE test_single (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 16;
SYSTEM STOP MERGES test_single;
INSERT INTO test_single SELECT 16 * number FROM numbers_mt(1e4);
SELECT 'single partition, allow only:';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_single SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT 'single partition, forced:';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_single SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_single SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_single SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1));
DROP TABLE test_single;

-- too few partitions (4 < max_threads/2 = 8): rejected; force bypasses it
DROP TABLE IF EXISTS test_too_few;
CREATE TABLE test_too_few (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 4;
SYSTEM STOP MERGES test_too_few;
INSERT INTO test_too_few SELECT number FROM numbers_mt(1e4);
SELECT 'too few partitions, allow only:';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_too_few SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT 'too few partitions, forced:';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_too_few SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_too_few SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_too_few SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1));
DROP TABLE test_too_few;

-- too many partitions (8 > max_number_of_partitions_for_independent_distinct = 4): rejected; force bypasses it
DROP TABLE IF EXISTS test_too_many;
CREATE TABLE test_too_many (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_too_many;
INSERT INTO test_too_many SELECT number FROM numbers_mt(1e4);
SELECT 'too many partitions, allow only:';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_too_many SETTINGS allow_distinct_partitions_independently = 1, max_number_of_partitions_for_independent_distinct = 4) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT 'too many partitions, forced:';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_too_many SETTINGS allow_distinct_partitions_independently = 1, max_number_of_partitions_for_independent_distinct = 4, force_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_too_many SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_too_many SETTINGS allow_distinct_partitions_independently = 1, max_number_of_partitions_for_independent_distinct = 4, force_distinct_partitions_independently = 1));
DROP TABLE test_too_many;

-- skewed partitions (one partition holds ~90% of rows): rejected; force bypasses it
DROP TABLE IF EXISTS test_skew;
CREATE TABLE test_skew (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_skew;
INSERT INTO test_skew SELECT if(number % 10 < 9, 0, number % 8) FROM numbers_mt(1e4);
SELECT 'skewed partitions, allow only:';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_skew SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT 'skewed partitions, forced:';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_skew SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_skew SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_skew SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1));
DROP TABLE test_skew;
