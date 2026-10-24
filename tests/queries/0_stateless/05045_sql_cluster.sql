-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

DROP CLUSTER IF EXISTS test_sql_cluster_05045;

CREATE CLUSTER test_sql_cluster_05045 (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000)
    )
);

SELECT shard_num, replica_num, host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05045'
ORDER BY shard_num, replica_num
FORMAT TSV;

ALTER CLUSTER test_sql_cluster_05045 (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000),
        REPLICA (host = '127.0.0.2', port = 9000)
    )
);

SELECT shard_num, replica_num, host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05045'
ORDER BY shard_num, replica_num
FORMAT TSV;

SYSTEM DROP DNS CACHE;

SELECT shard_num, replica_num, host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05045'
ORDER BY shard_num, replica_num
FORMAT TSV;

DROP CLUSTER test_sql_cluster_05045;

SELECT count() FROM system.clusters WHERE cluster = 'test_sql_cluster_05045';

CREATE CLUSTER test_sql_cluster_05045 (
    user = 'default',
    SHARD (
        host = '127.0.0.1',
        port = 9000
    )
);

SELECT shard_num, replica_num, host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05045'
ORDER BY shard_num, replica_num
FORMAT TSV;

DROP CLUSTER test_sql_cluster_05045;

SELECT count() FROM system.clusters WHERE cluster = 'test_sql_cluster_05045';

CREATE CLUSTER test_sql_cluster_05045 (
    user = 'default',
    SHARD (
        weight = 2,
        internal_replication = 1,
        REPLICA (host = '127.0.0.1', port = 9000),
        REPLICA (host = '127.0.0.2', port = 9000)
    )
);

SELECT shard_num, replica_num, host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05045'
ORDER BY shard_num, replica_num
FORMAT TSV;

DROP CLUSTER IF EXISTS test_sql_cluster_05045;

CREATE CLUSTER test_sql_cluster_05045 (
    user = 'default',
    SHARD (
        host = '127.0.0.1',
        REPLICA (host = '127.0.0.2', port = 9000)
    )
); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_sql_cluster_05045 (
    user = 'default',
    SHARD (
        REPLICA (weight = 2, host = '127.0.0.1', port = 9000)
    )
); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_sql_cluster_05045 (
    weight = 2,
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000)
    )
); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_shard_localhost (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000)
    )
); -- { serverError CLUSTER_ALREADY_EXISTS }
