-- Tags: shard

DROP TABLE IF EXISTS data_03988;
DROP TABLE IF EXISTS dist_03988;
DROP VIEW IF EXISTS view_03988;

CREATE TABLE data_03988 (key Int, value String) ENGINE = MergeTree() ORDER BY key;
INSERT INTO data_03988 SELECT number, toString(number) FROM numbers(10);

CREATE TABLE dist_03988 AS data_03988
    ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data_03988, key % 2);

CREATE VIEW view_03988 AS SELECT * FROM dist_03988;

SET optimize_skip_unused_shards = 1;
SET force_optimize_skip_unused_shards = 1;

-- Direct query on distributed table should work
SELECT * FROM dist_03988 WHERE key = 0 ORDER BY key;

-- Query through view should also work (this was the bug)
SELECT * FROM view_03988 WHERE key = 0 ORDER BY key;

DROP VIEW view_03988;
DROP TABLE dist_03988;
DROP TABLE data_03988;
