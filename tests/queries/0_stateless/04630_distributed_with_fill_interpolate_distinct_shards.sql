-- Tags: no-parallel, shard

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111728 (distinct-shard-values
-- variant). Two shards hold DISJOINT rows, so a per-shard WITH FILL / INTERPOLATE (the bug) would fill
-- the gaps and interpolate against each shard's own neighbours and then concatenate, producing extra
-- rows and wrong interpolated values. The correct result fills once over the globally merged stream:
-- g in {1,2,3,5} across the shards, so only g = 0 and g = 4 are synthesized, and id is interpolated
-- from the global predecessor (default -> 1 for g = 0, 30 -> 31 for g = 4). Both processing stages are
-- exercised: the default WithMergeableStateAfterAggregationAndLimit and, with
-- distributed_push_down_limit = 0, WithMergeableStateAfterAggregation.

DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

CREATE TABLE shard_0.t (id UInt32, g UInt16) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t (id UInt32, g UInt16) ENGINE = MergeTree ORDER BY id;
INSERT INTO shard_0.t VALUES (10, 1), (30, 3);
INSERT INTO shard_1.t VALUES (20, 2), (40, 5);

CREATE TABLE d (id UInt32, g UInt16)
    ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t, id);

-- serialize_query_plan = 0 because WITH FILL is not supported in serialized sort descriptions
-- (serializeSortDescription throws NOT_IMPLEMENTED) and the CI `distributed plan` shard turns
-- serialize_query_plan on globally; this test exercises the shard read, not plan serialization.
SET serialize_query_plan = 0;

SELECT g, id FROM d ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + 1);
SELECT g, id FROM d ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + 1)
SETTINGS distributed_push_down_limit = 0;

DROP TABLE d;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
