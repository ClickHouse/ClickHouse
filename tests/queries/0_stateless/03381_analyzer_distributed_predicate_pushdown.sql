-- https://github.com/ClickHouse/ClickHouse/issues/76182

SELECT toDayOfWeek(toDateTime(0) + uid)
FROM cluster(test_cluster_two_shards, default.users)
FORMAT Null;

SELECT toDayOfWeek(toDateTime(0) + uid) in (7)
FROM cluster(test_cluster_two_shards, default.users)
FORMAT Null;

