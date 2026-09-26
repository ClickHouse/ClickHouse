-- `parallel_replicas_filter_pushdown` is the way out if this push-down misbehaves in production: with
-- it off, a condition standing above the part of the query parallel replicas execute stays there, and
-- the fragment is shipped as it was written - what versions before 26.10 did.
--
-- Both halves have to go together. The condition entering the initiator's copy of the fragment and the
-- condition spliced into the query the replicas are sent are one decision: ship it to them while the
-- initiator keeps it above the read, and they would filter - and order their read - by something the
-- initiator never applied there, announcing a different coordination mode. So the switch is checked on
-- both: the fragment's own plan, and the query the replicas were sent.

DROP TABLE IF EXISTS t_pr_pushdown_switch;
DROP VIEW IF EXISTS v_pr_pushdown_switch;

CREATE TABLE t_pr_pushdown_switch (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO t_pr_pushdown_switch SELECT number % 100, number FROM numbers(10000);

CREATE VIEW v_pr_pushdown_switch AS SELECT tenant, ts FROM t_pr_pushdown_switch ORDER BY ts;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET parallel_replicas_allow_view_over_mergetree = 1;
SET optimize_read_in_order = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;
SET parallel_replicas_plan_based = 0;

SELECT 'on by default: the condition is in the fragment';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM v_pr_pushdown_switch WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';

SELECT 'off: it stays above the read';
SET parallel_replicas_filter_pushdown = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM v_pr_pushdown_switch WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';

SELECT 'and the answer does not change either way';
SELECT count() FROM v_pr_pushdown_switch WHERE tenant = 5;
SET parallel_replicas_filter_pushdown = 1;
SELECT count() FROM v_pr_pushdown_switch WHERE tenant = 5;

-- The replicas must not be sent the condition while the initiator keeps it above the read. Read from a
-- run without a local plan, where the initiator has to consume the replicas' streams to the end, so
-- their queries are logged before it returns. A replica query is scoped by the database it reads:
-- `current_database` is logged as `default` for it, while `databases` holds this test's.
SET parallel_replicas_filter_pushdown = 0;
SELECT tenant, ts FROM v_pr_pushdown_switch WHERE tenant = 5 ORDER BY ts
SETTINGS log_comment = '05256_switch_off', parallel_replicas_local_plan = 0 FORMAT Null;

SET parallel_replicas_filter_pushdown = 1;
SELECT tenant, ts FROM v_pr_pushdown_switch WHERE tenant = 5 ORDER BY ts
SETTINGS log_comment = '05256_switch_on', parallel_replicas_local_plan = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'the replicas were sent the condition only with the switch on';
SELECT log_comment, count() > 0 AS replicas_were_sent_a_query, countIf(query LIKE '%HAVING%equals(%tenant%') > 0 AS carrying_it
FROM system.query_log
WHERE has(databases, currentDatabase()) AND log_comment IN ('05256_switch_off', '05256_switch_on')
  AND NOT is_initial_query
GROUP BY log_comment ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;

DROP VIEW v_pr_pushdown_switch;
DROP TABLE t_pr_pushdown_switch;
