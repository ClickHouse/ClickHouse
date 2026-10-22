-- max_rows_to_read_leaf should not apply to local (non-distributed) queries
-- See https://github.com/ClickHouse/ClickHouse/issues/99268

-- parallel replicas make a query distributed, so they are enabled only where the test asks for them
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_leaf_limit;
CREATE TABLE t_leaf_limit (number UInt32) ENGINE = MergeTree ORDER BY number;
INSERT INTO t_leaf_limit SELECT number FROM numbers(10000000);

CREATE VIEW v_leaf_limit AS SELECT * FROM t_leaf_limit;

-- Direct table access should work (leaf limit should not apply locally)
SELECT count() FROM t_leaf_limit SETTINGS max_rows_to_read_leaf = 5000000;

-- View with ORDER BY should work
SELECT count() FROM (SELECT * FROM v_leaf_limit ORDER BY number LIMIT 3) SETTINGS max_rows_to_read_leaf = 5000000;

-- View without ORDER BY should also work (this was the failing case)
SELECT count() FROM (SELECT * FROM v_leaf_limit LIMIT 3) SETTINGS max_rows_to_read_leaf = 5000000;

SELECT count() FROM numbers(1000) SETTINGS max_rows_to_read_leaf = 10;

DROP VIEW v_leaf_limit;
DROP TABLE t_leaf_limit;

-- the workers of a cluster table function are leaves: they limit the read of the IN subquery
-- (a `file` read itself does not check storage limits)
INSERT INTO FUNCTION file(currentDatabase() || '_04045_leaf.tsv', 'TSV', 'x UInt64') SELECT number FROM numbers(1000)
SETTINGS engine_file_truncate_on_insert = 1;
SELECT sum(x) FROM file(currentDatabase() || '_04045_leaf.tsv', 'TSV', 'x UInt64')
WHERE x IN (SELECT number FROM numbers(100000)) SETTINGS max_rows_to_read_leaf = 10;
SELECT sum(x) FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_04045_leaf.tsv', 'TSV', 'x UInt64')
WHERE x IN (SELECT number FROM numbers(100000)) SETTINGS max_rows_to_read_leaf = 10; -- { serverError TOO_MANY_ROWS }

-- the local replica of plan-based parallel replicas is a leaf: it stops early like the remote replicas
DROP TABLE IF EXISTS t_leaf_pr;
CREATE TABLE t_leaf_pr (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100;
INSERT INTO t_leaf_pr SELECT number FROM numbers(1000000);
SELECT count() < 100000 FROM t_leaf_pr WHERE NOT ignore(a)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 1,
    max_threads = 1, max_block_size = 100, max_rows_to_read_leaf = 1000, read_overflow_mode_leaf = 'break';
DROP TABLE t_leaf_pr;
