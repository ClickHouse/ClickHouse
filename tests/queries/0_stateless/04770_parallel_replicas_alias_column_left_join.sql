-- Tags: no-random-settings

-- An `ALIAS` column of the nullable side of an outer `JOIN` must be the default of its type for unmatched rows,
-- not the alias expression applied to the default of the base column.

DROP TABLE IF EXISTS t_alias_left_join;
CREATE TABLE t_alias_left_join (k Int64, x Int64, a String ALIAS toString(x)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_alias_left_join SELECT number, number + 1 FROM numbers(10);

SELECT l.k, r.a FROM t_alias_left_join AS l LEFT JOIN t_alias_left_join AS r ON l.k = r.k + 100 ORDER BY l.k LIMIT 2
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT l.k, r.a FROM t_alias_left_join AS l LEFT JOIN t_alias_left_join AS r ON l.k = r.k + 100 ORDER BY l.k LIMIT 2;

DROP TABLE t_alias_left_join;
