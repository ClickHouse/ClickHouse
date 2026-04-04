-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/85895
-- When two ALIAS columns share the same expression, the Distributed engine
-- must not deduplicate them into a single aggregate column.

DROP TABLE IF EXISTS shard_dup_alias;
DROP TABLE IF EXISTS dist_dup_alias;

CREATE TABLE shard_dup_alias (a String, b Float64, c Float64, d Float64 ALIAS b + c, e Float64 ALIAS b + c) ENGINE = MergeTree() ORDER BY a;
INSERT INTO shard_dup_alias VALUES ('x', 1, 2);

CREATE TABLE dist_dup_alias (a String, b Float64, c Float64, d Float64 ALIAS b + c, e Float64 ALIAS b + c)
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_dup_alias, rand());

SELECT sum(d) AS f, sum(e) AS g FROM dist_dup_alias;

-- Also verify that selecting alias columns directly works.
SELECT d, e FROM dist_dup_alias;

DROP TABLE dist_dup_alias;
DROP TABLE shard_dup_alias;
