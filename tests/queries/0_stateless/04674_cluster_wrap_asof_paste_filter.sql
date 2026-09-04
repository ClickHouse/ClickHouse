-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- Copying a wrap `WHERE` onto `IStorageCluster` must not prefilter the right
-- side of an `ASOF JOIN` or either side of a `PASTE JOIN`.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_asof_left;
CREATE TABLE t_asof_left
(
    id Int32,
    t Int32
)
ENGINE = Memory;
INSERT INTO t_asof_left VALUES (1, 10);

INSERT INTO FUNCTION file(currentDatabase() || '_04674_asof_right.tsv', 'TSV', 'id Int32, t Int32, flag Int32')
SELECT *
FROM
(
    SELECT 1 AS id, 9 AS t, 0 AS flag
    UNION ALL
    SELECT 1, 8, 1
)
SETTINGS engine_file_truncate_on_insert = 1;

-- Nearest right row is `(t = 9, flag = 0)`. `WHERE flag = 1` must run after
-- `ASOF`, so the result is empty. Prefiltering the wrapped `fileCluster` would
-- keep only `t = 8` and incorrectly match it.
SELECT count()
FROM t_asof_left AS l
ASOF JOIN fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_04674_asof_right.tsv',
    'TSV',
    'id Int32, t Int32, flag Int32') AS r ON l.id = r.id AND l.t >= r.t
WHERE r.flag = 1;

DROP TABLE t_asof_left;

INSERT INTO FUNCTION file(currentDatabase() || '_04674_paste_left.tsv', 'TSV', 'n Int32, flag Int32')
SELECT number, if(number = 1, 1, 0)
FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_paste_right;
CREATE TABLE t_paste_right
(
    s String
)
ENGINE = Memory;
INSERT INTO t_paste_right VALUES ('a'), ('b'), ('c');

-- `PASTE` pairs by position, then `WHERE flag = 1` keeps the middle pair
-- `(1, b)`. Prefiltering the wrapped left table would pair `1` with `a`.
SELECT l.n, r.s
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_04674_paste_left.tsv',
    'TSV',
    'n Int32, flag Int32') AS l
PASTE JOIN t_paste_right AS r
WHERE l.flag = 1
SETTINGS max_threads = 1;

DROP TABLE t_paste_right;
