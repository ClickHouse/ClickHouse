DROP TABLE IF EXISTS default.x;

CREATE TABLE default.x AS system.numbers ENGINE = MergeTree ORDER BY number;
CREATE TABLE default.y AS system.numbers ENGINE = MergeTree ORDER BY number;

-- Just one shard, sharding key isn't necessary
INSERT INTO FUNCTION cluster('test_shard_localhost', default, x) SELECT * FROM numbers(10);
INSERT INTO FUNCTION cluster('test_shard_localhost', default, x, rand()) SELECT * FROM numbers(10);

-- More than one shard, sharding key is necessary
INSERT INTO FUNCTION cluster('test_cluster_two_shards_localhost', default, x) SELECT * FROM numbers(10); --{ serverError 55 }
INSERT INTO FUNCTION cluster('test_cluster_two_shards_localhost', default, x, rand()) SELECT * FROM numbers(10);

INSERT INTO FUNCTION remote('127.0.0.{1,2}', default, y, 'default') SELECT * FROM numbers(10); -- { serverError 55 }
INSERT INTO FUNCTION remote('127.0.0.{1,2}', default, y, 'default', rand()) SELECT * FROM numbers(10);

SELECT * FROM default.x ORDER BY number;

SELECT * FROM remote('127.0.0.{1,2}', default, y) ORDER BY number;

DROP TABLE default.x;
DROP TABLE default.y;
