-- Tags: shard

-- Verify that the optimized trivial count produces correct AggregateFunction
-- argument types when querying through a Distributed table. With incorrect
-- argument types, the initiator expects AggregateFunction(count, UInt64) but
-- shards send AggregateFunction(count, UInt32, UInt64), causing a type mismatch
-- during aggregation state merging.

DROP TABLE IF EXISTS test_trivial_count_types_local;
DROP TABLE IF EXISTS test_trivial_count_types_dist;

CREATE TABLE test_trivial_count_types_local (v0 UInt32, v1 UInt64) ENGINE = MergeTree ORDER BY v0;
INSERT INTO test_trivial_count_types_local VALUES (1, 2), (3, 4);

CREATE TABLE test_trivial_count_types_dist AS test_trivial_count_types_local
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), test_trivial_count_types_local);

SET optimize_trivial_count_query = 1;
SET allow_experimental_analyzer = 1;

SELECT count(v0 + v1) FROM test_trivial_count_types_dist;

DROP TABLE test_trivial_count_types_dist;
DROP TABLE test_trivial_count_types_local;
