-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ

SET max_threads = 8;
-- The optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;

-- { echo }

-- When the partition key is a function of the DISTINCT key (so partitions can be read independently)
-- and the DISTINCT key is also a primary-key prefix (so it can be read in order), each partition is
-- read as its own sorted stream and DISTINCT runs the streaming transform per partition, in parallel.
-- There is no merge of all partitions into one stream.
DROP TABLE IF EXISTS test_partition_in_order;
CREATE TABLE test_partition_in_order (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY a % 8;
SYSTEM STOP MERGES test_partition_in_order;
INSERT INTO test_partition_in_order SELECT number % 100, number      FROM numbers(1000);
INSERT INTO test_partition_in_order SELECT number % 100, number+1000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_partition_in_order SETTINGS optimize_distinct_in_order = 1, allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT DISTINCT a FROM test_partition_in_order SETTINGS optimize_distinct_in_order = 1, allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Distinct%Transform%' OR explain LIKE '%Resize%';
SELECT count() = (SELECT uniqExact(a) FROM test_partition_in_order) FROM (SELECT DISTINCT a FROM test_partition_in_order SETTINGS optimize_distinct_in_order = 1, allow_distinct_partitions_independently = 1);

-- Multi-column DISTINCT key covering the full primary key.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a, b FROM test_partition_in_order SETTINGS optimize_distinct_in_order = 1, allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT count() = (SELECT uniqExact((a, b)) FROM test_partition_in_order) FROM (SELECT DISTINCT a, b FROM test_partition_in_order SETTINGS optimize_distinct_in_order = 1, allow_distinct_partitions_independently = 1);
DROP TABLE test_partition_in_order;

-- Contrast: with per-partition reading disabled the in-order optimization still applies, but the
-- partition streams are collapsed into one before the final DISTINCT (no "Skip stream merging").
DROP TABLE IF EXISTS test_in_order_no_partition;
CREATE TABLE test_in_order_no_partition (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_order_no_partition;
INSERT INTO test_in_order_no_partition SELECT number % 100, number      FROM numbers(1000);
INSERT INTO test_in_order_no_partition SELECT number % 100, number+1000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_in_order_no_partition SETTINGS optimize_distinct_in_order = 1, allow_distinct_partitions_independently = 0) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT count() = (SELECT uniqExact(a) FROM test_in_order_no_partition) FROM (SELECT DISTINCT a FROM test_in_order_no_partition SETTINGS optimize_distinct_in_order = 1, allow_distinct_partitions_independently = 0);
DROP TABLE test_in_order_no_partition;
