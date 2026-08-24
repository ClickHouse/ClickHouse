-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- `hostName` is `isServerConstant`. Copying it into the `IStorageCluster` wrap
-- would evaluate it on remotes, where it can differ from the initiator, and
-- drop every row. The `n < 2` conjunct must still be copied.

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

DROP TABLE t_04677_right;
