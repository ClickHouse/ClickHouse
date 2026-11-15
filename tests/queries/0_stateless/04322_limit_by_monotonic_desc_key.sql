-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings, no-random-merge-tree-settings, no-parallel-replicas: Explain output may differ

SET max_threads = 16;

-- { echo }

-- Descending (reverse) sorting key with a strict-monotonic function on the LIMIT BY column: reading in
-- order yields descending `a`, and negate(a) is monotonic, so the streaming transform still applies.
DROP TABLE IF EXISTS test_monotonic_desc;
CREATE TABLE test_monotonic_desc (a Int32, v UInt32) ENGINE = MergeTree ORDER BY (a DESC) SETTINGS allow_experimental_reverse_key = 1;
SYSTEM STOP MERGES test_monotonic_desc;
INSERT INTO test_monotonic_desc SELECT number % 50, number      FROM numbers(1000);
INSERT INTO test_monotonic_desc SELECT number % 50, number+1000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT negate(a) FROM test_monotonic_desc LIMIT 1 BY negate(a) SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
SELECT count() = (SELECT uniqExact(a) FROM test_monotonic_desc) FROM (SELECT negate(a) FROM test_monotonic_desc LIMIT 1 BY negate(a) SETTINGS optimize_limit_by_in_order = 1);
SELECT (SELECT groupArray(na) FROM (SELECT na FROM (SELECT negate(a) AS na FROM test_monotonic_desc LIMIT 1 BY negate(a) SETTINGS optimize_limit_by_in_order = 0) ORDER BY na)) = (SELECT groupArray(na) FROM (SELECT na FROM (SELECT negate(a) AS na FROM test_monotonic_desc LIMIT 1 BY negate(a) SETTINGS optimize_limit_by_in_order = 1) ORDER BY na));
DROP TABLE test_monotonic_desc;

-- Multi-column descending key (both columns DESC), LIMIT BY a monotonic function of the leading column.
DROP TABLE IF EXISTS test_monotonic_desc_multi;
CREATE TABLE test_monotonic_desc_multi (a Int32, b UInt32, v UInt32) ENGINE = MergeTree ORDER BY (a DESC, b DESC) SETTINGS allow_experimental_reverse_key = 1;
SYSTEM STOP MERGES test_monotonic_desc_multi;
INSERT INTO test_monotonic_desc_multi SELECT number % 50, number % 7, number      FROM numbers(1000);
INSERT INTO test_monotonic_desc_multi SELECT number % 50, number % 7, number+1000 FROM numbers(1000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT negate(a) FROM test_monotonic_desc_multi LIMIT 1 BY negate(a) SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%' OR explain LIKE '%MergingSortedTransform%';
SELECT count() = (SELECT uniqExact(a) FROM test_monotonic_desc_multi) FROM (SELECT negate(a) FROM test_monotonic_desc_multi LIMIT 1 BY negate(a) SETTINGS optimize_limit_by_in_order = 1);
DROP TABLE test_monotonic_desc_multi;
