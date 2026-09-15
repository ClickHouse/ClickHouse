-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Stress test for the fallback decision reaching every context a plan captured. Set building for
-- `x IN (subquery)` reads `make_distributed_plan` live from the context the reading step captured; a plan
-- that falls back must therefore never build its sets as distributed plans, whichever code path built it:
-- the interpreter, a nested interpreter (view, scalar subquery, Buffer, Distributed local shard), a plan
-- driven by hand (GLOBAL IN / GLOBAL JOIN subquery), a plan kept aside (set source, materialized CTE, correlated
-- subquery), a plan deserialized on a shard (`serialize_query_plan`), or a plan spliced into the parent only during
-- optimization (`TimeSeries` read). Every query aggregates under a global GROUP BY limit, which a distributed plan
-- does not support, so each falls back; the positive control lifts the limit and must stay distributed, which
-- proves the detection.
-- Tasks are summed over every query of this run carrying the log_comment: shard queries inherit it and count
-- their own set builds, so the initiator row alone would miss them. The run is scoped by the initiators'
-- query ids, found through the test database, because log_comment values repeat across runs.

SET make_distributed_plan = 1;
SET max_rows_to_group_by = 100000000;
SET prefer_localhost_replica = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_materialized_cte = 1;
SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS t_dpc;
DROP TABLE IF EXISTS t_dpc2;
DROP TABLE IF EXISTS d_dpc;
DROP TABLE IF EXISTS m_dpc;
DROP TABLE IF EXISTS b_dpc;
DROP TABLE IF EXISTS sink_dpc;
DROP TABLE IF EXISTS src_dpc;
DROP TABLE IF EXISTS mut_dpc;
DROP TABLE IF EXISTS mv_dpc;
DROP TABLE IF EXISTS ts_dpc;
DROP TABLE IF EXISTS ts_dpc_tags;
DROP VIEW IF EXISTS v_dpc;
DROP VIEW IF EXISTS v2_dpc;
DROP VIEW IF EXISTS v3_dpc;

CREATE TABLE t_dpc (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_dpc SELECT number FROM numbers(100000);
CREATE TABLE t_dpc2 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_dpc2 SELECT number * 2 FROM numbers(50000);
CREATE TABLE d_dpc AS t_dpc ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_dpc);
CREATE TABLE m_dpc (x UInt64) ENGINE = Merge(currentDatabase(), '^t_dpc$');
CREATE TABLE b_dpc (x UInt64) ENGINE = Buffer(currentDatabase(), t_dpc, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
CREATE TABLE sink_dpc (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE src_dpc (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE mut_dpc (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mut_dpc SELECT number FROM numbers(100000);
CREATE VIEW v_dpc AS SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000);
CREATE VIEW v2_dpc AS SELECT x FROM v_dpc;
CREATE VIEW v3_dpc AS SELECT (SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) AS c;
CREATE MATERIALIZED VIEW mv_dpc TO sink_dpc AS SELECT x FROM src_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) GROUP BY x;
-- An external tags table gives the `TimeSeries` inner table a stable name for `additional_table_filters`.
CREATE TABLE ts_dpc_tags (`id` Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags)))), `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))), `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC')))) ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE ts_dpc (time_series Array(Tuple(DateTime64(3, 'UTC'), Float32))) ENGINE = TimeSeries TAGS ts_dpc_tags;
INSERT INTO ts_dpc (metric_name, tags, time_series)
    SELECT 'm' || toString(number % 3), map('k', toString(number)), [(toDateTime64('2025-11-30 10:30:05.125', 3, 'UTC'), toFloat32(number))] FROM numbers(100);

-- The main execution path, always covered: the control.
SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_01_top_level';
SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) WITH TOTALS SETTINGS max_rows_to_group_by = 0, log_comment = '05185_dpc_02_with_totals';
SELECT count() FROM t_dpc WHERE x + 0 IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_03_deferred_set';

-- Plans built by a nested interpreter and united into the parent.
SELECT count() FROM v_dpc SETTINGS log_comment = '05185_dpc_04_view';
SELECT count() FROM v2_dpc SETTINGS log_comment = '05185_dpc_05_view_in_view';
SELECT (SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SETTINGS log_comment = '05185_dpc_06_scalar_subquery';
SELECT c FROM v3_dpc SETTINGS log_comment = '05185_dpc_07_scalar_in_view';
SELECT count() FROM m_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_08_merge_table';
SELECT count() FROM b_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_09_buffer_table';

-- Shards: a local shard plan on the initiator, and shard queries that receive a serialized plan or a query text.
SELECT count() FROM d_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_10_distributed_local_shard';
SELECT count() FROM d_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS prefer_localhost_replica = 0, log_comment = '05185_dpc_11_distributed_remote_shards';
SELECT count() FROM d_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS serialize_query_plan = 1, log_comment = '05185_dpc_12_distributed_serialized_plan';
SELECT count() FROM d_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS serialize_query_plan = 0, log_comment = '05185_dpc_13_distributed_query_text';
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_dpc) WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_14_remote_function';
SELECT count() FROM cluster(test_cluster_two_shards, currentDatabase(), t_dpc) WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_15_cluster_function';
SELECT count() FROM d_dpc WHERE x IN (SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SETTINGS serialize_query_plan = 1, log_comment = '05185_dpc_15b_serialized_plan_nested_set';

-- Plans driven by hand after extraction: GLOBAL IN / GLOBAL JOIN subqueries.
SELECT count() FROM d_dpc WHERE x GLOBAL IN (SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) GROUP BY x) SETTINGS log_comment = '05185_dpc_16_global_in';
SELECT count() FROM d_dpc WHERE x GLOBAL IN (SELECT x FROM v_dpc GROUP BY x) SETTINGS log_comment = '05185_dpc_17_global_in_view_body';
SELECT count() FROM d_dpc GLOBAL INNER JOIN (SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) GROUP BY x) AS r USING (x) SETTINGS log_comment = '05185_dpc_18_global_join';

-- Plans kept aside and copied into the parent: set sources, CTEs, correlated subqueries.
-- A materialized CTE is kept aside only when referenced more than once; a single reference is inlined.
SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x IN (SELECT x FROM t_dpc WHERE x < 60000)) SETTINGS log_comment = '05185_dpc_19_nested_set_source';
WITH s AS (SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SELECT count() FROM s AS a INNER JOIN s AS b USING (x) SETTINGS log_comment = '05185_dpc_20_cte_twice';
WITH s AS MATERIALIZED (SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SELECT count() FROM (SELECT x FROM s UNION ALL SELECT x FROM s) SETTINGS log_comment = '05185_dpc_21_materialized_cte_union';
WITH s AS MATERIALIZED (SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SELECT count() FROM s AS a INNER JOIN s AS b ON a.x = b.x SETTINGS log_comment = '05185_dpc_21b_materialized_cte_join';
SELECT count() FROM t_dpc AS a WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) AND EXISTS (SELECT 1 FROM t_dpc2 AS b WHERE b.x = a.x) SETTINGS allow_experimental_correlated_subqueries = 1, log_comment = '05185_dpc_22_correlated';

-- Plan shapes: union branches, a join with the IN on the right side, an IN over a table.
SELECT c FROM (SELECT count() AS c FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) UNION ALL SELECT count() AS c FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x >= 60000)) ORDER BY c SETTINGS log_comment = '05185_dpc_23_union_all';
SELECT count() FROM t_dpc AS a INNER JOIN (SELECT x FROM t_dpc2 WHERE x IN (SELECT x FROM t_dpc WHERE x < 60000)) AS b USING (x) SETTINGS log_comment = '05185_dpc_24_join_right_in';
SELECT count() FROM t_dpc WHERE x IN t_dpc2 SETTINGS log_comment = '05185_dpc_25_in_table';

-- Other statement kinds: INSERT SELECT, an INSERT that pushes through a materialized view, EXPLAIN, a mutation.
INSERT INTO sink_dpc SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) GROUP BY x SETTINGS log_comment = '05185_dpc_26_insert_select';
INSERT INTO src_dpc SELECT number FROM numbers(1000) SETTINGS log_comment = '05185_dpc_27_insert_through_mv';
SELECT count() FROM sink_dpc;
SELECT count() > 0 FROM (EXPLAIN PLAN indexes = 1 SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SETTINGS log_comment = '05185_dpc_28_explain_plan';
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SETTINGS log_comment = '05185_dpc_29_explain_pipeline';
SELECT count() > 0 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SETTINGS log_comment = '05185_dpc_30_explain_estimate';
ALTER TABLE mut_dpc DELETE WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS mutations_sync = 2, log_comment = '05185_dpc_31_alter_delete';
SELECT count() FROM mut_dpc;

-- A `TimeSeries` read plans its generated inner query aside and the optimizer splices that sub-plan into the outer plan
-- after the outer decision (the wrapper step is not serializable, so the outer plan always falls back). The sub-plan
-- must follow: no exchanges or logical joins left in it, and an IN set inside it (applied through
-- `additional_table_filters` on the tags table) must be built locally.
SELECT tags['k'] AS k, length(time_series) FROM ts_dpc ORDER BY toUInt32(k) LIMIT 3 SETTINGS log_comment = '05185_dpc_32_time_series_read';
SELECT countIf(explain LIKE '%Exchange%') AS exchanges, countIf(explain LIKE '%JoinLogical%') AS logical_joins
FROM (EXPLAIN PLAN SELECT tags['k'] AS k FROM ts_dpc ORDER BY k LIMIT 3);
SELECT metric_name, count() FROM ts_dpc GROUP BY metric_name ORDER BY metric_name
    SETTINGS additional_table_filters = {'ts_dpc_tags': 'metric_name IN (SELECT \'m\' || toString(x) FROM t_dpc WHERE x < 60000)'}, log_comment = '05185_dpc_33_time_series_filter_set';

-- Strict mode: the fallback itself is asserted, the query throws instead of running locally.
SELECT count() FROM v_dpc SETTINGS distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

-- Positive controls: without the GROUP BY limit the same shapes stay distributed.
SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS max_rows_to_group_by = 0, log_comment = '05185_dpc_positive_top_level';
SELECT count() FROM v_dpc SETTINGS max_rows_to_group_by = 0, log_comment = '05185_dpc_positive_view';

SYSTEM FLUSH LOGS query_log;

-- Every fallen-back query: no distributed-plan task may have been spawned anywhere under its log_comment.
WITH (SELECT groupArray(query_id) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '05185_dpc_%') AS run_ids
SELECT log_comment, sum(ProfileEvents['DistributedPlanRemoteTasks']) AS remote_tasks
FROM system.query_log
WHERE type = 'QueryFinish' AND has(run_ids, initial_query_id) AND log_comment LIKE '05185_dpc_%' AND log_comment NOT LIKE '05185_dpc_positive_%'
GROUP BY log_comment
ORDER BY log_comment;

-- The positive controls did spawn tasks, so the counter really measures what we think it measures.
WITH (SELECT groupArray(query_id) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '05185_dpc_positive_%') AS run_ids
SELECT log_comment, sum(ProfileEvents['DistributedPlanRemoteTasks']) > 0 AS distributed
FROM system.query_log
WHERE type = 'QueryFinish' AND has(run_ids, initial_query_id) AND log_comment LIKE '05185_dpc_positive_%'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE ts_dpc;
DROP TABLE ts_dpc_tags;
DROP TABLE mv_dpc;
DROP VIEW v3_dpc;
DROP VIEW v2_dpc;
DROP VIEW v_dpc;
DROP TABLE mut_dpc;
DROP TABLE src_dpc;
DROP TABLE sink_dpc;
DROP TABLE b_dpc;
DROP TABLE m_dpc;
DROP TABLE d_dpc;
DROP TABLE t_dpc2;
DROP TABLE t_dpc;
