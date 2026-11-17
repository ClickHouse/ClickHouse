-- The `Remote`, `RemoteSecure` and `Cluster` database engines reject an empty remote database name,
-- and the cluster name `all_groups.` (an empty database name after the prefix) names no cluster.

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Remote('127.0.0.1', ''); -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = RemoteSecure('127.0.0.1', ''); -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Cluster(test_shard_localhost, ''); -- { serverError BAD_ARGUMENTS }
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Remote('127.0.0.1', ''); -- { serverError BAD_ARGUMENTS }
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Cluster(test_shard_localhost, ''); -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Cluster('all_groups.', 'default'); -- { serverError CLUSTER_DOESNT_EXIST }
SELECT * FROM cluster('all_groups.', system.one); -- { serverError CLUSTER_DOESNT_EXIST }
