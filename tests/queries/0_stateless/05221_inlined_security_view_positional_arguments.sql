-- The analyzer inlines a view (`analyzer_inline_views`) with a context built by
-- `StorageView::getViewSubqueryContext`. A view is expanded on the node that executes it, so
-- positional arguments in the stored view body have never been resolved by the initiator: the
-- context must be marked as a view inner query, exactly like on the `StorageView::read` path,
-- or `QueryAnalyzer::replaceNodesWithPositionalArguments` skips them on a shard
-- (`SECONDARY_QUERY`) and on a local plan of the initiator (`prefer_localhost_replica = 1`).
-- `GROUP BY 1` then stays a literal (`NOT_AN_AGGREGATE`), and `ORDER BY 1` sorts by a constant.
-- A projection-only `SQL SECURITY DEFINER` / `NONE` view stays inlineable under
-- `sql_security_views_are_optimization_barriers`, so it is covered together with `INVOKER`.

SET enable_analyzer = 1, analyzer_inline_views = 1, enable_positional_arguments = 1;
SET max_threads = 1, query_plan_remove_redundant_sorting = 0;

DROP TABLE IF EXISTS t_05221 SYNC;
CREATE TABLE t_05221 (k UInt8, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05221 SELECT number % 3, number FROM numbers(6);

CREATE VIEW v_05221_group_invoker SQL SECURITY INVOKER AS SELECT k, count() AS c FROM t_05221 GROUP BY 1;
CREATE VIEW v_05221_group_definer SQL SECURITY DEFINER AS SELECT k, count() AS c FROM t_05221 GROUP BY 1;
CREATE VIEW v_05221_group_none SQL SECURITY NONE AS SELECT k, count() AS c FROM t_05221 GROUP BY 1;

CREATE VIEW v_05221_order_invoker SQL SECURITY INVOKER AS SELECT v, k FROM t_05221 ORDER BY 1 DESC;
CREATE VIEW v_05221_order_definer SQL SECURITY DEFINER AS SELECT v, k FROM t_05221 ORDER BY 1 DESC;
CREATE VIEW v_05221_order_none SQL SECURITY NONE AS SELECT v, k FROM t_05221 ORDER BY 1 DESC;

SELECT '-- GROUP BY 1 through a remote shard: 3 groups';
SELECT count() FROM remote('127.0.0.1', currentDatabase(), v_05221_group_invoker) SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), v_05221_group_definer) SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), v_05221_group_none) SETTINGS prefer_localhost_replica = 0;

SELECT '-- GROUP BY 1 through a local plan: 3 groups';
SELECT count() FROM remote('127.0.0.1', currentDatabase(), v_05221_group_invoker) SETTINGS prefer_localhost_replica = 1;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), v_05221_group_definer) SETTINGS prefer_localhost_replica = 1;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), v_05221_group_none) SETTINGS prefer_localhost_replica = 1;

SELECT '-- ORDER BY 1 DESC through a remote shard: v descending';
SELECT arrayStringConcat(groupArray(toString(v)), ',') FROM remote('127.0.0.1', currentDatabase(), v_05221_order_invoker) SETTINGS prefer_localhost_replica = 0;
SELECT arrayStringConcat(groupArray(toString(v)), ',') FROM remote('127.0.0.1', currentDatabase(), v_05221_order_definer) SETTINGS prefer_localhost_replica = 0;
SELECT arrayStringConcat(groupArray(toString(v)), ',') FROM remote('127.0.0.1', currentDatabase(), v_05221_order_none) SETTINGS prefer_localhost_replica = 0;

SELECT '-- ORDER BY 1 DESC through a local plan: v descending';
SELECT arrayStringConcat(groupArray(toString(v)), ',') FROM remote('127.0.0.1', currentDatabase(), v_05221_order_invoker) SETTINGS prefer_localhost_replica = 1;
SELECT arrayStringConcat(groupArray(toString(v)), ',') FROM remote('127.0.0.1', currentDatabase(), v_05221_order_definer) SETTINGS prefer_localhost_replica = 1;
SELECT arrayStringConcat(groupArray(toString(v)), ',') FROM remote('127.0.0.1', currentDatabase(), v_05221_order_none) SETTINGS prefer_localhost_replica = 1;

DROP VIEW v_05221_group_invoker;
DROP VIEW v_05221_group_definer;
DROP VIEW v_05221_group_none;
DROP VIEW v_05221_order_invoker;
DROP VIEW v_05221_order_definer;
DROP VIEW v_05221_order_none;
DROP TABLE t_05221 SYNC;
