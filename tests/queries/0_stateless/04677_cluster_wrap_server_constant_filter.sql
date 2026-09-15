-- Tags: no-fasttest, no-parallel-replicas, no-random-settings
-- no-fasttest: `fileCluster` is not in the fast test build.
-- no-parallel-replicas: EXPLAIN of the cluster wrap differs with parallel replicas.
-- no-random-settings: join / filter EXPLAIN text is randomized otherwise.
--
-- `hostName` is `isServerConstant` and must not be copied into the wrap query
-- sent to remotes (`ReadFromCluster` "Query:" line). `count()` cannot see that:
-- `hostName() = hostName()` is true on every node.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04677_wrap_left.tsv', 'TSV', 'n UInt64')
SELECT number
FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_04677_right;
CREATE TABLE t_04677_right
(
    n UInt64
)
ENGINE = Memory;
INSERT INTO t_04677_right VALUES (0), (1), (2);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_04677_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_04677_right AS r ON l.n = r.n
WHERE l.n < 2 AND hostName() = hostName();

SELECT throwIf(count() = 0)
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM fileCluster(
        'test_cluster_one_shard_two_replicas',
        currentDatabase() || '_04677_wrap_left.tsv',
        'TSV',
        'n UInt64') AS l
    LEFT JOIN t_04677_right AS r ON l.n = r.n
    WHERE l.n < 2 AND hostName() = hostName()
)
WHERE explain LIKE '%Query:%'
    AND (explain LIKE '%n < 2%' OR explain LIKE '%less(%2%')
FORMAT Null;

SELECT throwIf(count() != 0)
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM fileCluster(
        'test_cluster_one_shard_two_replicas',
        currentDatabase() || '_04677_wrap_left.tsv',
        'TSV',
        'n UInt64') AS l
    LEFT JOIN t_04677_right AS r ON l.n = r.n
    WHERE l.n < 2 AND hostName() = hostName()
)
WHERE explain LIKE '%Query:%' AND explain ILIKE '%hostName%'
FORMAT Null;

DROP TABLE t_04677_right;
