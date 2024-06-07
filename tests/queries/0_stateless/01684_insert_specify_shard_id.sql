-- Tags: shard

DROP TABLE IF EXISTS x;
DROP TABLE IF EXISTS x_dist;
DROP TABLE IF EXISTS y;
DROP TABLE IF EXISTS y_dist;

CREATE TABLE x AS system.numbers ENGINE = MergeTree ORDER BY number;
CREATE TABLE y AS system.numbers ENGINE = MergeTree ORDER BY number;

CREATE TABLE x_dist as x ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), x);
CREATE TABLE y_dist as y ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), y);

-- insert into first shard
INSERT INTO x_dist SELECT * FROM numbers(10) settings insert_shard_id = 1;
INSERT INTO y_dist SELECT * FROM numbers(10) settings insert_shard_id = 1;

SELECT * FROM x_dist ORDER by number;
SELECT * FROM y_dist ORDER by number;

-- insert into second shard
INSERT INTO x_dist SELECT * FROM numbers(10, 10) settings insert_shard_id = 2;
INSERT INTO y_dist SELECT * FROM numbers(10, 10) settings insert_shard_id = 2;

SELECT * FROM x_dist ORDER by number;
SELECT * FROM y_dist ORDER by number;

-- no sharding key
INSERT INTO x_dist SELECT * FROM numbers(10); -- { serverError STORAGE_REQUIRES_PARAMETER }
INSERT INTO y_dist SELECT * FROM numbers(10); -- { serverError STORAGE_REQUIRES_PARAMETER }

-- invalid shard id
INSERT INTO x_dist SELECT * FROM numbers(10) settings insert_shard_id = 3; -- { serverError INVALID_SHARD_ID }
INSERT INTO y_dist SELECT * FROM numbers(10) settings insert_shard_id = 3; -- { serverError INVALID_SHARD_ID }

DROP TABLE x;
DROP TABLE x_dist;
DROP TABLE y;
DROP TABLE y_dist;
