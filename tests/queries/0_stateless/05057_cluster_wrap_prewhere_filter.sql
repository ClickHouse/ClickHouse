-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- Left-only `PREWHERE` on a wrapped `IStorageCluster` JOIN must be copied onto
-- the wrap (as wrap `WHERE`, because cluster storages do not support `PREWHERE`)
-- so wrap dummy analysis sees the same predicate as a copied `WHERE`.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_prewhere = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05057_wrap_left.tsv', 'TSV', 'n UInt64')
SELECT number
FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_05057_right;
CREATE TABLE t_05057_right
(
    n UInt64
)
ENGINE = Memory;
INSERT INTO t_05057_right VALUES (0), (1), (2);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05057_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_05057_right AS r ON l.n = r.n
PREWHERE l.n < 2;

SELECT count()
FROM
(
    SELECT *
    FROM fileCluster(
        'test_cluster_one_shard_two_replicas',
        currentDatabase() || '_05057_wrap_left.tsv',
        'TSV',
        'n UInt64') AS l
    LEFT JOIN t_05057_right AS r ON l.n = r.n
    PREWHERE l.n < 2
);

SELECT
    (SELECT count()
     FROM fileCluster(
         'test_cluster_one_shard_two_replicas',
         currentDatabase() || '_05057_wrap_left.tsv',
         'TSV',
         'n UInt64') AS l
     LEFT JOIN t_05057_right AS r ON l.n = r.n
     PREWHERE l.n < 2)
    = (SELECT count()
       FROM fileCluster(
           'test_cluster_one_shard_two_replicas',
           currentDatabase() || '_05057_wrap_left.tsv',
           'TSV',
           'n UInt64') AS l
       LEFT JOIN t_05057_right AS r ON l.n = r.n
       WHERE l.n < 2);

-- `ANY INNER JOIN` cannot prefilter either side because doing so can change
-- which duplicate survives. Since the wrap cannot preserve this `PREWHERE`,
-- retain the storage's normal `ILLEGAL_PREWHERE` rejection.
SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05057_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
ANY INNER JOIN t_05057_right AS r ON l.n = r.n
PREWHERE l.n < 2; -- { serverError ILLEGAL_PREWHERE }

-- A partially copyable `PREWHERE` cannot leave an unsafe conjunct behind:
-- unlike `WHERE`, there is no post-JOIN filter step that would apply it.
SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05057_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_05057_right AS r ON l.n = r.n
PREWHERE l.n < 2 AND hostName() = hostName(); -- { serverError ILLEGAL_PREWHERE }

-- Without a JOIN wrap, cluster storages still do not support `PREWHERE`.
SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05057_wrap_left.tsv',
    'TSV',
    'n UInt64')
PREWHERE n < 2; -- { serverError ILLEGAL_PREWHERE }

DROP TABLE t_05057_right;
