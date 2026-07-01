-- Tags: no-random-settings, no-parallel-replicas
-- no-random-settings: Explain output may differ; no-parallel-replicas: avoids duplicated plan steps

SET enable_analyzer = 1;
SET query_plan_push_limit_by_into_sort = 1;
SET max_threads = 16;
SET explain_query_plan_default = 'legacy';

-- { echo }

-- Permuted LIMIT BY pushed into the sort: ORDER BY (key, val) covers the LIMIT BY keys (val, key) in a
-- different order. The optimizer installs the per-stream LIMIT BY hint with the permuted columns, and at
-- runtime (several sorted streams) the per-stream LimitBySortedStreamTransform pre-cap runs before the merge.
DROP TABLE IF EXISTS test_push_permuted;
CREATE TABLE test_push_permuted (key UInt32, val UInt32) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES test_push_permuted;
INSERT INTO test_push_permuted SELECT number % 20, number % 7 FROM numbers(1000);
INSERT INTO test_push_permuted SELECT number % 20, number % 7 FROM numbers(1000);
INSERT INTO test_push_permuted SELECT number % 20, number % 7 FROM numbers(1000);
SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN actions = 1 SELECT key, val FROM test_push_permuted ORDER BY key, val LIMIT 1 BY val, key) WHERE explain LIKE '%Per-stream LIMIT BY%';
SELECT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT key, val FROM test_push_permuted ORDER BY key, val LIMIT 1 BY val, key) WHERE explain ILIKE '%LimitBySorted%' OR explain ILIKE '%MergingSorted%';
SELECT (SELECT groupArray((key, val)) FROM (SELECT key, val FROM test_push_permuted ORDER BY key, val LIMIT 1 BY val, key SETTINGS query_plan_push_limit_by_into_sort = 0)) = (SELECT groupArray((key, val)) FROM (SELECT key, val FROM test_push_permuted ORDER BY key, val LIMIT 1 BY val, key SETTINGS query_plan_push_limit_by_into_sort = 1));
DROP TABLE test_push_permuted;

-- Partial cover (ORDER BY key while LIMIT BY key, val) installs no per-stream pre-cap.
DROP TABLE IF EXISTS test_push_partial;
CREATE TABLE test_push_partial (key UInt32, val UInt32) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES test_push_partial;
INSERT INTO test_push_partial SELECT number % 20, number % 7 FROM numbers(1000);
INSERT INTO test_push_partial SELECT number % 20, number % 7 FROM numbers(1000);
SELECT count() FROM (EXPLAIN actions = 1 SELECT key, val FROM test_push_partial ORDER BY key LIMIT 1 BY key, val) WHERE explain LIKE '%Per-stream LIMIT BY%';
SELECT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT key, val FROM test_push_partial ORDER BY key LIMIT 1 BY key, val) WHERE explain ILIKE '%LimitBySorted%' OR explain ILIKE '%MergingSorted%';
DROP TABLE test_push_partial;
