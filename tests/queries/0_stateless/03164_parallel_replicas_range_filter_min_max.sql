DROP TABLE IF EXISTS range_filter_custom_range_test;

CREATE TABLE range_filter_custom_range_test (k UInt64) ENGINE=MergeTree ORDER BY k;

INSERT INTO range_filter_custom_range_test SELECT number + 5 from numbers(10);

SELECT count()
FROM
(
    SELECT *
    FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), range_filter_custom_range_test)
    SETTINGS prefer_localhost_replica = 0,
        max_parallel_replicas = 3,
        distributed_group_by_no_merge = 0,
        enable_parallel_replicas = 1,
        parallel_replicas_custom_key = 'k',
        parallel_replicas_mode = 'custom_key_range',
        parallel_replicas_custom_key_range_lower = 5,
        parallel_replicas_custom_key_range_upper = 15
);

SELECT count()
FROM
(
    SELECT *
    FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), range_filter_custom_range_test)
    SETTINGS prefer_localhost_replica = 0,
        max_parallel_replicas = 3,
        distributed_group_by_no_merge = 0,
        enable_parallel_replicas = 1,
        parallel_replicas_custom_key = 'k',
        parallel_replicas_mode = 'custom_key_range',
        parallel_replicas_custom_key_range_lower = 4,
        parallel_replicas_custom_key_range_upper = 14
);

SELECT count()
FROM
(
    SELECT *
    FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), range_filter_custom_range_test)
    SETTINGS prefer_localhost_replica = 0,
        max_parallel_replicas = 3,
        distributed_group_by_no_merge = 0,
        enable_parallel_replicas = 1,
        parallel_replicas_custom_key = 'k',
        parallel_replicas_mode = 'custom_key_range',
        parallel_replicas_custom_key_range_lower = 6,
        parallel_replicas_custom_key_range_upper = 17
);


SELECT count()
FROM
(
    SELECT *
    FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), range_filter_custom_range_test)
    SETTINGS prefer_localhost_replica = 0,
        max_parallel_replicas = 3,
        distributed_group_by_no_merge = 0,
        enable_parallel_replicas = 1,
        parallel_replicas_custom_key = 'k',
        parallel_replicas_mode = 'custom_key_range',
        parallel_replicas_custom_key_range_lower = 0,
        parallel_replicas_custom_key_range_upper = 15
);

SELECT count()
FROM
(
    SELECT *
    FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), range_filter_custom_range_test)
    SETTINGS prefer_localhost_replica = 0,
        max_parallel_replicas = 3,
        distributed_group_by_no_merge = 0,
        enable_parallel_replicas = 1,
        parallel_replicas_custom_key = 'k',
        parallel_replicas_mode = 'custom_key_range',
        parallel_replicas_custom_key_range_lower = 15,
        parallel_replicas_custom_key_range_upper = 25
);

SELECT count()
FROM
(
    SELECT *
    FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), range_filter_custom_range_test)
    SETTINGS prefer_localhost_replica = 0,
        max_parallel_replicas = 3,
        distributed_group_by_no_merge = 0,
        enable_parallel_replicas = 1,
        parallel_replicas_custom_key = 'k',
        parallel_replicas_mode = 'custom_key_range',
        parallel_replicas_custom_key_range_lower = 0,
        parallel_replicas_custom_key_range_upper = 5
);

SELECT count()
FROM
(
    SELECT *
    FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), range_filter_custom_range_test)
    SETTINGS prefer_localhost_replica = 0,
        max_parallel_replicas = 3,
        distributed_group_by_no_merge = 0,
        enable_parallel_replicas = 1,
        parallel_replicas_custom_key = 'k',
        parallel_replicas_mode = 'custom_key_range',
        parallel_replicas_custom_key_range_lower = 500,
        parallel_replicas_custom_key_range_upper = 10000
);


SELECT count()
FROM
(
    SELECT *
    FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), range_filter_custom_range_test)
    SETTINGS prefer_localhost_replica = 0,
        max_parallel_replicas = 3,
        distributed_group_by_no_merge = 0,
        enable_parallel_replicas = 1,
        parallel_replicas_custom_key = 'k',
        parallel_replicas_mode = 'custom_key_range',
        parallel_replicas_custom_key_range_lower = 10,
        parallel_replicas_custom_key_range_upper = 13
);

DROP TABLE range_filter_custom_range_test;

DROP TABLE IF EXISTS range_filter_custom_range_test_2;

CREATE TABLE range_filter_custom_range_test_2 (k UInt64) ENGINE=MergeTree ORDER BY k;

INSERT INTO range_filter_custom_range_test_2 SELECT number from numbers(13);

SELECT count()
FROM
(
    SELECT *
    FROM cluster(parallel_replicas, currentDatabase(), range_filter_custom_range_test_2)
    SETTINGS prefer_localhost_replica = 0,
        max_parallel_replicas = 12,
        distributed_group_by_no_merge = 0,
        enable_parallel_replicas = 1,
        parallel_replicas_custom_key = 'k',
        parallel_replicas_mode = 'custom_key_range',
        parallel_replicas_custom_key_range_lower = 0,
        parallel_replicas_custom_key_range_upper = 13
);

DROP TABLE range_filter_custom_range_test_2;

DROP TABLE IF EXISTS range_filter_custom_range_test_3;

CREATE TABLE range_filter_custom_range_test_3 (k UInt64) ENGINE=MergeTree ORDER BY k;

INSERT INTO range_filter_custom_range_test_3 SELECT number from numbers(4);

SELECT count()
FROM
(
    SELECT *
    FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), range_filter_custom_range_test_3)
    SETTINGS prefer_localhost_replica = 0,
        max_parallel_replicas = 12,
        distributed_group_by_no_merge = 0,
        enable_parallel_replicas = 1,
        parallel_replicas_custom_key = 'k',
        parallel_replicas_mode = 'custom_key_range',
        parallel_replicas_custom_key_range_lower = 0,
        parallel_replicas_custom_key_range_upper = 4
);

DROP TABLE range_filter_custom_range_test_3;
