-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- `IN` / `GLOBAL IN` over a local (temporary) table is node-local. Copying it
-- onto the `IStorageCluster` wrap sends the table name to remotes, which do
-- not see initiator temporary tables. The `n < 2` conjunct must still be
-- copied. Tuple `IN` is not node-local and must still be copied.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET prefer_localhost_replica = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05056_wrap_left.tsv', 'TSV', 'n UInt64')
SELECT number
FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_05056_right;
CREATE TABLE t_05056_right
(
    n UInt64
)
ENGINE = Memory;
INSERT INTO t_05056_right VALUES (0), (1), (2);

DROP TABLE IF EXISTS tmp_05056_set;
CREATE TEMPORARY TABLE tmp_05056_set
(
    n UInt64
);
INSERT INTO tmp_05056_set VALUES (0), (1);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05056_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_05056_right AS r ON l.n = r.n
WHERE l.n < 2 AND l.n IN tmp_05056_set;

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05056_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_05056_right AS r ON l.n = r.n
WHERE l.n < 2 AND l.n GLOBAL IN tmp_05056_set;

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05056_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_05056_right AS r ON l.n = r.n
WHERE l.n IN (0, 1);

DROP TABLE tmp_05056_set;
DROP TABLE t_05056_right;
