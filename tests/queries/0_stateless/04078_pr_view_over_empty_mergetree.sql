-- Test that querying a view over an empty MergeTree table does not cause a
-- logical error when parallel replicas is enabled. The view's inner query
-- produces ReadNothingStep instead of ReadFromMergeTree for empty tables.

DROP TABLE IF EXISTS t_empty;
DROP VIEW IF EXISTS v_empty;

CREATE TABLE t_empty (key UInt64, value UInt64) ENGINE = MergeTree() ORDER BY key;
CREATE VIEW v_empty AS SELECT * FROM t_empty;

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT sum(value) FROM v_empty SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

DROP VIEW v_empty;
DROP TABLE t_empty;
