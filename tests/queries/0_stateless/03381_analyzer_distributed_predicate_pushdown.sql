-- https://github.com/ClickHouse/ClickHouse/issues/76182

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);

SELECT toDayOfWeek(toDateTime(0) + uid)
FROM cluster(test_cluster_two_shards, currentDatabase(), users)
FORMAT Null;

SELECT toDayOfWeek(toDateTime(0) + uid) in (7)
FROM cluster(test_cluster_two_shards, currentDatabase(), users)
FORMAT Null;

