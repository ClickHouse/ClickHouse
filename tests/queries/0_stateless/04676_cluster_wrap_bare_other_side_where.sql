-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- A wrap `WHERE` that is a bare column from the other JOIN side must be dropped
-- rather than copied onto `SELECT cols FROM fileCluster`. Otherwise planning
-- fails with an unknown identifier.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04676_wrap_left.tsv', 'TSV', 'n Int32')
SELECT number + 1
FROM numbers(2)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_04676_right;
CREATE TABLE t_04676_right
(
    id Int32,
    flag UInt8
)
ENGINE = Memory;
INSERT INTO t_04676_right VALUES (1, 1), (2, 0);

SELECT l.n
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_04676_wrap_left.tsv',
    'TSV',
    'n Int32') AS l
INNER JOIN t_04676_right AS r ON l.n = r.id
WHERE r.flag
ORDER BY l.n;

DROP TABLE t_04676_right;
