DROP TABLE IF EXISTS default.x;

CREATE TABLE default.x ON CLUSTER test_cluster_two_shards_localhost AS system.numbers ENGINE = Log;

INSERT INTO FUNCTION cluster('test_cluster_two_shards_localhost', default, x, rand()) SELECT * FROM numbers(10);

DROP TABLE default.x ON CLUSTER test_cluster_two_shards_localhost;
