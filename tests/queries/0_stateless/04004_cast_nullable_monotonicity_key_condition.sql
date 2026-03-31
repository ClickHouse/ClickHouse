-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- EXPLAIN output may differ

SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET optimize_trivial_count_query = 1;

DROP TABLE IF EXISTS test_nullable_filter;
CREATE TABLE test_nullable_filter (x UInt32, y UInt32) ENGINE=MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO test_nullable_filter SELECT number, number FROM numbers(1000);

SELECT count()
FROM test_nullable_filter
WHERE x::Nullable(UInt64) > 500;

EXPLAIN indexes = 1
SELECT count()
FROM test_nullable_filter
WHERE x::Nullable(UInt64) > 500;
