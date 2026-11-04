-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Stress test for the `make_distributed_plan` fallback decision.
-- A plan that falls back to local execution must never
-- build its `x IN (subquery)` sets as distributed plans, whichever code path built the plan: the interpreter, a nested
-- interpreter (view, scalar subquery, Buffer, Distributed local shard), a plan driven by hand (GLOBAL IN subquery), a plan
-- kept aside (set source, materialized CTE, correlated subquery), a plan deserialized on a shard (`serialize_query_plan`),
-- or a plan spliced into the parent only during optimization (`TimeSeries` read). Every query aggregates under a global
-- GROUP BY limit, which a distributed plan does not support, so each falls back.
-- The other group lifts the limit  of max_rows_to_group_by, hence
-- must stay distributed.
-- Next to the task count each row prints the reasons the initiator and the shard queries logged when they fell back
-- (`system.text_log`), so a changed plan shape shows up as a changed reason, not only as a changed count.

SET make_distributed_plan = 1;
SET max_rows_to_group_by = 100000000;
SET prefer_localhost_replica = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_materialized_cte = 1;
-- Pinned for a stable fallback reason on the shards: a shard that receives a serialized plan falls back on the GROUP BY
-- limit, a shard that receives query text on its `BlocksMarshalling` step. Only the distributed-plan CI jobs set this in
-- users.d; row 13 flips it to cover the query-text path.
SET serialize_query_plan = 1;
-- Randomized by the test runner; it decides which step the shard of row 13 falls back on.
SET enable_parallel_blocks_marshalling = 1;
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
CREATE VIEW v3_dpc AS SELECT (SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) AS c;
CREATE MATERIALIZED VIEW mv_dpc TO sink_dpc AS SELECT x FROM src_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) GROUP BY x;
-- An external tags table gives the `TimeSeries` inner table a stable name for `additional_table_filters`.
CREATE TABLE ts_dpc_tags (`id` Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags)))), `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))), `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC')))) ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE ts_dpc (samples Array(Tuple(DateTime64(3, 'UTC'), Float32))) ENGINE = TimeSeries TAGS ts_dpc_tags;
INSERT INTO ts_dpc (metric_name, tags, samples)
    SELECT 'm' || toString(number % 3), map('k', toString(number)), [(toDateTime64('2025-11-30 10:30:05.125', 3, 'UTC'), toFloat32(number))] FROM numbers(100);

-- Plain query, the baseline that always worked.
SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_01_top_level';
-- `x + 0` keeps the set out of index analysis, so it is built at execution time.
SELECT count() FROM t_dpc WHERE x + 0 IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_03_deferred_set';

-- Plans built by a nested interpreter: view, scalar subquery, scalar subquery in a view, Merge, Buffer.
SELECT count() FROM v_dpc SETTINGS log_comment = '05185_dpc_04_view';
SELECT (SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SETTINGS log_comment = '05185_dpc_06_scalar_subquery';
SELECT c FROM v3_dpc SETTINGS log_comment = '05185_dpc_07_scalar_in_view';
SELECT count() FROM m_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_08_merge_table';
SELECT count() FROM b_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_09_buffer_table';

-- Shards: the local shard plan on the initiator, a shard that receives a serialized plan, a shard that receives the
-- query text, and a set inside a set source of a serialized plan.
SELECT count() FROM d_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS log_comment = '05185_dpc_10_distributed_local_shard';
SELECT count() FROM d_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS serialize_query_plan = 1, log_comment = '05185_dpc_12_distributed_serialized_plan';
SELECT count() FROM d_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS serialize_query_plan = 0, log_comment = '05185_dpc_13_distributed_query_text';
SELECT count() FROM d_dpc WHERE x IN (SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SETTINGS serialize_query_plan = 1, log_comment = '05185_dpc_15b_serialized_plan_nested_set';

-- GLOBAL IN subqueries, plain and with a view body.
SELECT count() FROM d_dpc WHERE x GLOBAL IN (SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) GROUP BY x) SETTINGS log_comment = '05185_dpc_16_global_in';
SELECT count() FROM d_dpc WHERE x GLOBAL IN (SELECT x FROM v_dpc GROUP BY x) SETTINGS log_comment = '05185_dpc_17_global_in_view_body';

-- Plans kept aside: a set inside a set source, a CTE, a materialized CTE, a correlated subquery.
-- The materialized CTE is referenced twice: a single reference is inlined.
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
-- EXPLAIN never executes the plan, so no counter can fire: those rows only check that the decision path does not throw.
INSERT INTO sink_dpc SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) GROUP BY x SETTINGS log_comment = '05185_dpc_26_insert_select';
INSERT INTO src_dpc SELECT number FROM numbers(1000) SETTINGS log_comment = '05185_dpc_27_insert_through_mv';
SELECT count() FROM sink_dpc;
SELECT count() > 0 FROM (EXPLAIN PLAN indexes = 1 SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SETTINGS log_comment = '05185_dpc_28_explain_plan';
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SETTINGS log_comment = '05185_dpc_29_explain_pipeline';
SELECT count() > 0 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000)) SETTINGS log_comment = '05185_dpc_30_explain_estimate';
ALTER TABLE mut_dpc DELETE WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS mutations_sync = 2, log_comment = '05185_dpc_31_alter_delete';
SELECT count() FROM mut_dpc;
-- Assert mutations with their in set follow the fallback mechanism
ALTER TABLE mut_dpc DELETE WHERE x + 1 IN (SELECT x FROM v_dpc) SETTINGS mutations_sync = 2, log_comment = '05185_dpc_31b_alter_delete_set_over_view';
SELECT count() FROM mut_dpc;
ALTER TABLE mut_dpc DELETE WHERE x IN (SELECT x FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x >= 60000)) SETTINGS mutations_sync = 2, log_comment = '05185_dpc_31c_alter_delete_nested_set';
SELECT count() FROM mut_dpc;

-- A `TimeSeries` read: the outer plan always falls back, the generated sub-plan must come out local (no exchanges, no
-- logical joins), and an IN set inside it, applied through `additional_table_filters` on the tags table, must be built locally.
SELECT tags['k'] AS k, length(samples) FROM ts_dpc ORDER BY toUInt32(k) LIMIT 3 SETTINGS log_comment = '05185_dpc_32_time_series_read';
SELECT countIf(explain LIKE '%Exchange%') AS exchanges, countIf(explain LIKE '%JoinLogical%') AS logical_joins
FROM (EXPLAIN PLAN SELECT tags['k'] AS k FROM ts_dpc ORDER BY k LIMIT 3);
SELECT metric_name, count() FROM ts_dpc GROUP BY metric_name ORDER BY metric_name
    SETTINGS additional_table_filters = {'ts_dpc_tags': 'metric_name IN (SELECT \'m\' || toString(x) FROM t_dpc WHERE x < 60000)'}, log_comment = '05185_dpc_33_time_series_filter_set';

-- Strict mode: the fallback itself is asserted, the query throws instead of running locally.
SELECT count() FROM v_dpc SETTINGS distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

-- Positive controls: without the GROUP BY limit the same shapes stay distributed.
SELECT count() FROM t_dpc WHERE x IN (SELECT x FROM t_dpc2 WHERE x < 60000) SETTINGS max_rows_to_group_by = 0, log_comment = '05185_dpc_positive_top_level';
SELECT count() FROM v_dpc SETTINGS max_rows_to_group_by = 0, log_comment = '05185_dpc_positive_view';

SYSTEM FLUSH LOGS query_log, text_log;

-- Every fallen-back query: no distributed-plan task may have been spawned anywhere under its log_comment, and the
-- reasons logged on the initiator and on the shard queries are printed.
WITH (SELECT metadata_modification_time FROM system.tables WHERE database = currentDatabase() AND name = 't_dpc') AS run_start,
    (SELECT groupArray(query_id) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '05185_dpc_%' AND event_time >= run_start) AS run_ids,
    family AS (
        SELECT query_id, log_comment, is_initial_query, ProfileEvents['DistributedPlanRemoteTasks'] AS tasks
        FROM system.query_log
        WHERE type = 'QueryFinish' AND has(run_ids, initial_query_id) AND log_comment LIKE '05185_dpc_%' AND log_comment NOT LIKE '05185_dpc_positive_%'),
    reasons AS (
        SELECT f.log_comment, f.is_initial_query,
            replaceRegexpOne(replaceRegexpOne(extract(t.message, 'falling back to local execution: (.*)$'),
                '^make_distributed_plan (does not support |cannot distribute this query: it contains the step )', ''),
                '( which could not execute remotely|: the limit cannot.*)$', '') AS reason
        FROM system.text_log AS t INNER JOIN family AS f ON t.query_id = f.query_id
        -- Checks only runs from run_start, so it is fixed to this run
        WHERE t.event_date >= toDate(run_start) AND t.event_time >= run_start AND t.logger_name = 'makeDistributedPlan' AND t.message LIKE '%falling back to local execution%')
SELECT f.log_comment, sum(f.tasks) AS remote_tasks,
    (SELECT arraySort(groupUniqArray(reason)) FROM reasons WHERE reasons.log_comment = f.log_comment AND is_initial_query) AS initiator_fell_back_on,
    (SELECT arraySort(groupUniqArray(reason)) FROM reasons WHERE reasons.log_comment = f.log_comment AND NOT is_initial_query) AS shards_fell_back_on
FROM family AS f
GROUP BY f.log_comment
ORDER BY f.log_comment;

-- The positive controls did spawn tasks, so the counter really measures what we think it measures.
WITH (SELECT metadata_modification_time FROM system.tables WHERE database = currentDatabase() AND name = 't_dpc') AS run_start,
    (SELECT groupArray(query_id) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '05185_dpc_positive_%' AND event_time >= run_start) AS run_ids
SELECT log_comment, sum(ProfileEvents['DistributedPlanRemoteTasks']) > 0 AS distributed
FROM system.query_log
WHERE type = 'QueryFinish' AND has(run_ids, initial_query_id) AND log_comment LIKE '05185_dpc_positive_%'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE ts_dpc;
DROP TABLE ts_dpc_tags;
DROP TABLE mv_dpc;
DROP VIEW v3_dpc;
DROP VIEW v_dpc;
DROP TABLE mut_dpc;
DROP TABLE src_dpc;
DROP TABLE sink_dpc;
DROP TABLE b_dpc;
DROP TABLE m_dpc;
DROP TABLE d_dpc;
DROP TABLE t_dpc2;
DROP TABLE t_dpc;
