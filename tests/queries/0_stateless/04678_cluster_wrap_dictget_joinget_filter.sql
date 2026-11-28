-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: `fileCluster` is not in the fast test build.
-- no-parallel-replicas: dictionaries created here are not on parallel replica workers.
-- `dictGet` / `joinGet` / `FQDN` are not safe to copy onto the `IStorageCluster` wrap
-- `WHERE` (node-local dictionary, Join table, or hostname). The `n < 2` conjunct
-- must still be copied.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04678_wrap_left.tsv', 'TSV', 'n UInt64')
SELECT number
FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_04678_right;
CREATE TABLE t_04678_right
(
    n UInt64
)
ENGINE = Memory;
INSERT INTO t_04678_right VALUES (0), (1), (2);

DROP DICTIONARY IF EXISTS dict_04678;
CREATE DICTIONARY dict_04678
(
    id UInt64,
    flag UInt8
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY $$SELECT c1 AS id, c2 AS flag FROM VALUES((0, 1), (1, 1), (2, 0))$$))
LAYOUT(FLAT())
LIFETIME(0);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_04678_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_04678_right AS r ON l.n = r.n
WHERE l.n < 2 AND dictGet(currentDatabase() || '.dict_04678', 'flag', l.n) = 1;

DROP TABLE IF EXISTS j_04678;
CREATE TABLE j_04678
(
    id UInt64,
    flag UInt8
)
ENGINE = Join(ANY, LEFT, id);
INSERT INTO j_04678 VALUES (0, 1), (1, 1), (2, 0);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_04678_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_04678_right AS r ON l.n = r.n
WHERE l.n < 2 AND joinGet(currentDatabase() || '.j_04678', 'flag', l.n) = 1;

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_04678_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_04678_right AS r ON l.n = r.n
WHERE l.n < 2 AND FQDN() = FQDN();

DROP TABLE j_04678;
DROP DICTIONARY dict_04678;
DROP TABLE t_04678_right;
