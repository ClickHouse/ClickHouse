DROP TABLE IF EXISTS x;

CREATE TABLE x AS system.numbers ENGINE = MergeTree ORDER BY number;

CREATE TABLE x_dist as x ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), x);

-- insert into first shard
INSERT INTO x_dist SELECT * FROM numbers(10) settings insert_shard_id = 1;

-- insert into second shard
INSERT INTO x_dist SELECT * FROM numbers(10, 10) settings insert_shard_id = 2;

-- invalid shard id
INSERT INTO x_dist SELECT * FROM numbers(10) settings insert_shard_id = 3; -- { serverError 1003 }

SELECT * FROM remote('127.0.0.1', currentDatabase(), x);
SELECT * FROM remote('127.0.0.2', currentDatabase(), x);

SELECT * FROM x_dist ORDER by number;

DROP TABLE x;
DROP TABLE x_dist;
