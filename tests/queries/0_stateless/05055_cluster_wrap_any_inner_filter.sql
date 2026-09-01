-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- Copying a wrap `WHERE` onto `IStorageCluster` must not prefilter either side
-- of an `ANY INNER JOIN`. Same invariant as `04812_any_inner_join_filter_push_down`.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05055_left.tsv', 'TSV', 'k UInt64, a UInt64')
SELECT *
FROM
(
    SELECT 1 AS k, 100 AS a
    UNION ALL
    SELECT 2, 200
    UNION ALL
    SELECT 1, 200
)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_05055_right;
CREATE TABLE t_05055_right
(
    k UInt64,
    v UInt64
)
ENGINE = Memory;
INSERT INTO t_05055_right VALUES (1, 10), (1, 20), (2, 30);

-- Two left rows share `k = 1` (`a = 100` and `a = 200`). Wrap prefilter of
-- `a = 200` would change which duplicate `ANY` keeps.
SELECT
    (SELECT count()
     FROM fileCluster(
         'test_cluster_one_shard_two_replicas',
         currentDatabase() || '_05055_left.tsv',
         'TSV',
         'k UInt64, a UInt64') AS l
     ANY INNER JOIN t_05055_right AS r ON l.k = r.k
     WHERE l.a = 200)
    = (SELECT sum(l.a = 200)
       FROM fileCluster(
           'test_cluster_one_shard_two_replicas',
           currentDatabase() || '_05055_left.tsv',
           'TSV',
           'k UInt64, a UInt64') AS l
       ANY INNER JOIN t_05055_right AS r ON l.k = r.k);

DROP TABLE t_05055_right;

DROP TABLE IF EXISTS t_05055_left;
CREATE TABLE t_05055_left
(
    k UInt64,
    a UInt64
)
ENGINE = Memory;
INSERT INTO t_05055_left VALUES (1, 100), (2, 200), (1, 200);

INSERT INTO FUNCTION file(currentDatabase() || '_05055_right.tsv', 'TSV', 'k UInt64, v UInt64')
SELECT *
FROM
(
    SELECT 1 AS k, 10 AS v
    UNION ALL
    SELECT 1, 20
    UNION ALL
    SELECT 2, 30
)
SETTINGS engine_file_truncate_on_insert = 1;

SELECT
    (SELECT count()
     FROM t_05055_left AS l
     ANY INNER JOIN fileCluster(
         'test_cluster_one_shard_two_replicas',
         currentDatabase() || '_05055_right.tsv',
         'TSV',
         'k UInt64, v UInt64') AS r ON l.k = r.k
     WHERE r.v > 10)
    = (SELECT sum(r.v > 10)
       FROM t_05055_left AS l
       ANY INNER JOIN fileCluster(
           'test_cluster_one_shard_two_replicas',
           currentDatabase() || '_05055_right.tsv',
           'TSV',
           'k UInt64, v UInt64') AS r ON l.k = r.k);

DROP TABLE t_05055_left;
