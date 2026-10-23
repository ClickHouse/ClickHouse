-- Tags: distributed

-- A `Buffer` unites its destination with the rows still in the buffers, so a query over it must be merged above
-- both even when the destination is a `Distributed` table that one shard finishes alone.
CREATE TABLE 05353_dst (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO 05353_dst VALUES (10, 'x'), (11, 'z');
CREATE TABLE 05353_dist AS 05353_dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), 05353_dst);
CREATE TABLE 05353_buf AS 05353_dst ENGINE = Buffer(currentDatabase(), 05353_dist, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO 05353_buf VALUES (10, 'x'), (1, 'y');

SELECT id, s, count() FROM 05353_buf GROUP BY id, s ORDER BY ALL;
SELECT sum(id), count() FROM 05353_buf;
SELECT id FROM 05353_buf ORDER BY id LIMIT 1;
SELECT DISTINCT id FROM 05353_buf ORDER BY id;
SELECT id FROM 05353_buf ORDER BY id LIMIT 1 BY id;
SELECT id, row_number() OVER (ORDER BY id, s) FROM 05353_buf ORDER BY ALL;
SELECT sum(id), count() FROM 05353_buf SETTINGS prefer_localhost_replica = 0;
SELECT sum(id), count() FROM 05353_buf SETTINGS prefer_localhost_replica = 1;
SELECT id, count() FROM 05353_buf GROUP BY id ORDER BY id SETTINGS distributed_group_by_no_merge = 1;
SELECT id, count() FROM 05353_buf GROUP BY id ORDER BY id SETTINGS distributed_group_by_no_merge = 2;

-- A `Merge` table takes its stage from its only child.
SELECT sum(id), count() FROM merge(currentDatabase(), '^05353_buf$');

-- A sharded destination whose shards finish a GROUP BY by the sharding key alone.
CREATE TABLE 05353_dist2 AS 05353_dst ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), 05353_dst, id);
CREATE TABLE 05353_buf2 AS 05353_dst ENGINE = Buffer(currentDatabase(), 05353_dist2, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO 05353_buf2 VALUES (10, 'x');
SELECT id, count() FROM 05353_buf2 WHERE id = 10 GROUP BY id SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1;

DROP TABLE 05353_buf;
DROP TABLE 05353_buf2;
DROP TABLE 05353_dist;
DROP TABLE 05353_dist2;
DROP TABLE 05353_dst;
