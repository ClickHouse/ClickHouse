-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings, no-random-merge-tree-settings, no-parallel-replicas: Explain output may differ

SET max_threads = 16;
SET explain_query_plan_default = 'legacy';
-- { echo }

-- When the partition key is a function of the LIMIT BY key (so partitions can be read independently)
-- and the LIMIT BY key is also a primary-key prefix (so it can be read in order), each partition is
-- read as its own sorted stream and LIMIT BY runs the streaming transform per partition, in parallel.
-- There is no merge of all partitions into one stream and no hash table.
DROP TABLE IF EXISTS test_partition_in_order;
CREATE TABLE test_partition_in_order (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY a % 8;
SYSTEM STOP MERGES test_partition_in_order;
INSERT INTO test_partition_in_order SELECT number % 100, number      FROM numbers(1000);
INSERT INTO test_partition_in_order SELECT number % 100, number+1000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_partition_in_order LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1, allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a FROM test_partition_in_order LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1, allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
SELECT count() = (SELECT uniqExact(a) FROM test_partition_in_order) FROM (SELECT a FROM test_partition_in_order LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1, allow_limit_by_partitions_independently = 1);

-- With OFFSET it stays correct.
SELECT (SELECT count() FROM (SELECT a FROM test_partition_in_order LIMIT 2 OFFSET 1 BY a SETTINGS optimize_limit_by_in_order = 0, allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a FROM test_partition_in_order LIMIT 2 OFFSET 1 BY a SETTINGS optimize_limit_by_in_order = 1, allow_limit_by_partitions_independently = 1));

-- Multi-column BY key covering the full primary key.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN actions = 1 SELECT a, b FROM test_partition_in_order LIMIT 1 BY a, b SETTINGS optimize_limit_by_in_order = 1, allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT count() = (SELECT uniqExact((a, b)) FROM test_partition_in_order) FROM (SELECT a, b FROM test_partition_in_order LIMIT 1 BY a, b SETTINGS optimize_limit_by_in_order = 1, allow_limit_by_partitions_independently = 1);
DROP TABLE test_partition_in_order;

-- Contrast: with per-partition reading disabled the in-order optimization still applies, but the
-- partition streams are collapsed into one before a single LIMIT BY (no "Skip stream merging").
DROP TABLE IF EXISTS test_in_order_no_partition;
CREATE TABLE test_in_order_no_partition (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_order_no_partition;
INSERT INTO test_in_order_no_partition SELECT number % 100, number      FROM numbers(1000);
INSERT INTO test_in_order_no_partition SELECT number % 100, number+1000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_in_order_no_partition LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1, allow_limit_by_partitions_independently = 0) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT count() = (SELECT uniqExact(a) FROM test_in_order_no_partition) FROM (SELECT a FROM test_in_order_no_partition LIMIT 1 BY a SETTINGS optimize_limit_by_in_order = 1, allow_limit_by_partitions_independently = 0);
DROP TABLE test_in_order_no_partition;
