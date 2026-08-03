-- Test that querying a view with SQL SECURITY NONE does not cause a logical
-- error when parallel replicas is enabled. The SQL SECURITY NONE path in
-- getSQLSecurityOverriddenContext creates a context from globalContext without
-- copying parallel replicas callbacks, which causes a crash when the inner
-- MergeTree read tries to use them on a follower replica.

DROP TABLE IF EXISTS t_base;
DROP VIEW IF EXISTS v_none;

CREATE TABLE t_base (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key SETTINGS index_granularity = 10;
INSERT INTO t_base SELECT number, number * 10 FROM numbers(1000);

CREATE VIEW v_none SQL SECURITY NONE AS SELECT * FROM t_base;

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT sum(value) FROM v_none SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

DROP VIEW v_none;
DROP TABLE t_base;
