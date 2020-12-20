DROP TABLE IF EXISTS default.x;

CREATE TABLE default.x ON CLUSTER test_shard_localhost AS system.numbers ENGINE = Log;

INSERT INTO FUNCTION cluster('test_shard_localhost', default, x) SELECT * FROM numbers(10);
-- In fact, in this case(just one shard), sharding key is not required
INSERT INTO FUNCTION cluster('test_shard_localhost', default, x, rand()) SELECT * FROM numbers(10);

SELECT * FROM default.x ORDER BY number;

DROP TABLE default.x ON CLUSTER test_shard_localhost;
