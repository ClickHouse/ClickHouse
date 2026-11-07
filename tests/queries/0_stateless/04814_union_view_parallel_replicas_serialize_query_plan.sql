-- Tags: no-random-settings, no-random-merge-tree-settings

-- With `parallel_replicas_allow_view_over_mergetree`, a view can expand into a `UNION ALL` whose
-- branches are all coordinated reads of one parallel-replicas fragment. This test pins that the
-- result stays correct (every branch returned exactly once, no per-replica duplication) when the
-- remote fragment is shipped as a serialized query plan (`serialize_query_plan = 1`) instead of an
-- AST: the shipped logical plan keeps a single `ReadFromTableStep` for the view itself, so marking
-- that step for parallel replicas covers every branch the replica expands it into. A review of
-- `createRemotePlanForParallelReplicas` suspected the branches past the first could degrade into
-- plain per-replica local reads on this path and duplicate their rows once per replica -- pin the
-- correct behavior for the plain, per-branch-`SETTINGS`, and nested-view shapes, against the
-- AST-shipped run of the very same queries.

DROP TABLE IF EXISTS t_union_view_ser_a;
DROP TABLE IF EXISTS t_union_view_ser_b;
DROP VIEW IF EXISTS v_union_view_ser;
DROP VIEW IF EXISTS v_union_view_ser_settings;
DROP VIEW IF EXISTS v_union_view_ser_nested;

CREATE TABLE t_union_view_ser_a (key UInt64) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t_union_view_ser_b (key UInt64) ENGINE = MergeTree ORDER BY key;

SYSTEM STOP MERGES t_union_view_ser_a;
SYSTEM STOP MERGES t_union_view_ser_b;

INSERT INTO t_union_view_ser_a SELECT number FROM numbers(1000);
INSERT INTO t_union_view_ser_a SELECT number + 1000 FROM numbers(1000);
INSERT INTO t_union_view_ser_b SELECT number + 100000 FROM numbers(1000);
INSERT INTO t_union_view_ser_b SELECT number + 101000 FROM numbers(1000);

CREATE VIEW v_union_view_ser AS
    SELECT key FROM t_union_view_ser_a
    UNION ALL
    SELECT key FROM t_union_view_ser_b;

-- Branch-scoped `SETTINGS` give every branch its own context, the shape where the initiator-local
-- fragment rebuilds each read under its branch scope.
CREATE VIEW v_union_view_ser_settings AS
    SELECT key FROM t_union_view_ser_a SETTINGS max_streams_for_merge_tree_reading = 16
    UNION ALL
    SELECT key FROM t_union_view_ser_b SETTINGS max_streams_for_merge_tree_reading = 1;

-- A view over a view: the fragment expands into nested `UNION ALL` levels.
CREATE VIEW v_union_view_ser_nested AS
    SELECT key FROM v_union_view_ser
    UNION ALL
    SELECT key FROM t_union_view_ser_a;

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_allow_view_over_mergetree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET max_threads = 4;

SELECT 'plain_union_view';
SELECT count(), sum(key) FROM (SELECT key FROM v_union_view_ser) SETTINGS serialize_query_plan = 0;
SELECT count(), sum(key) FROM (SELECT key FROM v_union_view_ser) SETTINGS serialize_query_plan = 1;

SELECT 'branch_settings_union_view';
SELECT count(), sum(key) FROM (SELECT key FROM v_union_view_ser_settings) SETTINGS serialize_query_plan = 0;
SELECT count(), sum(key) FROM (SELECT key FROM v_union_view_ser_settings) SETTINGS serialize_query_plan = 1;

SELECT 'nested_union_view';
SELECT count(), sum(key) FROM (SELECT key FROM v_union_view_ser_nested) SETTINGS serialize_query_plan = 0;
SELECT count(), sum(key) FROM (SELECT key FROM v_union_view_ser_nested) SETTINGS serialize_query_plan = 1;

-- The in-order path through the same fragment: the rows must come out sorted and complete.
SELECT 'order_by_union_view';
SELECT groupArray(key) = arraySort(groupArray(key)), count(), sum(key)
FROM (SELECT key FROM v_union_view_ser ORDER BY key)
SETTINGS serialize_query_plan = 1;

DROP VIEW v_union_view_ser_nested;
DROP VIEW v_union_view_ser_settings;
DROP VIEW v_union_view_ser;
DROP TABLE t_union_view_ser_a;
DROP TABLE t_union_view_ser_b;
