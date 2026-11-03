-- A condition above the fragment parallel replicas execute goes into the initiator's local copy of that
-- fragment and is spliced into the query the replicas run, conjunct by conjunct. Only what both sides
-- get may order the read: an equality fixes a sort key column, the read goes in order, and a side that
-- has it announces `WithOrder` to the shared coordinator against the other's `Default`. So a conjunct
-- the splice leaves out stays above the initiator's copy, unless it cannot fix a column - a range, a
-- join runtime filter (05056) - and then it only prunes. The shapes the splice refuses as a whole are
-- 05161 and 05183, a condition on a column the fragment computes 05366, and the switch's effect on the
-- replicas 03733.
-- https://github.com/ClickHouse/ClickHouse/issues/95524

DROP TABLE IF EXISTS t_pr_local_pd;
DROP TABLE IF EXISTS t_pr_local_pd_lambda_key;
DROP VIEW IF EXISTS v_pr_local_pd;
DROP VIEW IF EXISTS v_lambda_key_pr_local_pd;

CREATE TABLE t_pr_local_pd (tenant UInt64, arr Array(UInt64), ts UInt64)
    ENGINE = MergeTree ORDER BY (tenant, ts) SETTINGS index_granularity = 128;
INSERT INTO t_pr_local_pd SELECT number % 100, arrayMap(i -> i % 4, range(number % 8)), number FROM numbers(10000);

-- Sorted by a higher-order expression, so that the equality the splice cannot express - it drops a
-- lambda - is the one that would fix the key prefix.
CREATE TABLE t_pr_local_pd_lambda_key (arr Array(UInt64), ts UInt64)
    ENGINE = MergeTree ORDER BY (arrayCount(x -> x = 1, arr), ts) SETTINGS index_granularity = 128;
INSERT INTO t_pr_local_pd_lambda_key SELECT arrayMap(i -> i % 4, range(number % 8)), number FROM numbers(10000);

-- The sort has to be inside the fragment for the read to order itself at all.
CREATE VIEW v_pr_local_pd AS SELECT tenant, arr, ts FROM t_pr_local_pd ORDER BY ts;
CREATE VIEW v_lambda_key_pr_local_pd AS SELECT arr, ts FROM t_pr_local_pd_lambda_key ORDER BY ts;

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
-- What puts the view's read in the shipped fragment.
SET parallel_replicas_allow_view_over_mergetree = 1;
SET optimize_read_in_order = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
-- The splice writes the condition into the query the replicas are sent, so it needs this, and neither a
-- plan-based fragment nor a serialized plan carries it.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;
SET parallel_replicas_plan_based = 0;

SELECT 'an equality both sides get prunes the read and orders it';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_pr_local_pd WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_pr_local_pd WHERE tenant = 5;

SELECT 'a range prunes it and orders nothing';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_pr_local_pd WHERE tenant > 90 LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_pr_local_pd WHERE tenant > 90;

SELECT 'an equality the splice leaves out stays above the fragment';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT arr, ts FROM v_lambda_key_pr_local_pd WHERE arrayCount(x -> x = 1, arr) = 2 LIMIT 5
)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT arrayCount(x -> x = 1, arr), ts FROM v_lambda_key_pr_local_pd WHERE arrayCount(x -> x = 1, arr) = 2 ORDER BY ts LIMIT 3;

SELECT 'while the one beside it that the splice carries goes in, and orders the read';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM v_pr_local_pd WHERE tenant = 5 AND arrayCount(x -> x = 1, arr) = 1 LIMIT 5
)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT tenant, ts FROM v_pr_local_pd WHERE tenant = 5 AND arrayCount(x -> x = 1, arr) = 1 ORDER BY ts LIMIT 3;

SELECT 'also when it arrives as a condition of its own, after the other';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM (SELECT tenant, arr, ts FROM v_pr_local_pd WHERE arrayCount(x -> x = 1, arr) = 1) WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT tenant, ts FROM (SELECT tenant, arr, ts FROM v_pr_local_pd WHERE arrayCount(x -> x = 1, arr) = 1) WHERE tenant = 5 ORDER BY ts LIMIT 3;

SELECT 'the splice answers to the settings the fragment carries, not to the outer query';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM (
        SELECT tenant, ts FROM t_pr_local_pd ORDER BY ts SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 0
    ) WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT count() FROM (
    SELECT tenant, ts FROM t_pr_local_pd ORDER BY ts SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 0
) WHERE tenant = 5;
SET allow_push_predicate_ast_for_distributed_subqueries = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM (
        SELECT tenant, ts FROM t_pr_local_pd ORDER BY ts SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 1
    ) WHERE tenant = 5 LIMIT 5
)
WHERE explain LIKE '%Read type%';
SELECT count() FROM (
    SELECT tenant, ts FROM t_pr_local_pd ORDER BY ts SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 1
) WHERE tenant = 5;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;

SELECT 'with the push-down off the condition stays above, and the answer is the same';
SET parallel_replicas_filter_pushdown = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_pr_local_pd WHERE tenant = 5 LIMIT 5)
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Read type%';
SELECT count() FROM v_pr_local_pd WHERE tenant = 5;
SET parallel_replicas_filter_pushdown = 1;

-- The same conditions for the queries the replicas are sent, with the local plan off. With one, the
-- initiator's own share can cover the table, and the replicas are cancelled before they start and logged
-- after the flush below. Without one the initiator consumes their streams to the end, so they are logged
-- by the time it returns. The splice does not depend on there being a local plan.
SELECT arr, ts FROM v_lambda_key_pr_local_pd WHERE arrayCount(x -> x = 1, arr) = 2 ORDER BY ts
SETTINGS log_comment = '05055_lambda', parallel_replicas_local_plan = 0 FORMAT Null;
SELECT tenant, ts FROM v_pr_local_pd WHERE tenant = 5 AND arrayCount(x -> x = 1, arr) = 1 ORDER BY ts
SETTINGS log_comment = '05055_beside', parallel_replicas_local_plan = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- A replica query is scoped by the database it reads, not by `current_database`, which is logged as
-- `default` for it. `log_comment` travels with the query to the replicas. A replica cancelled before it
-- started logs `ExceptionBeforeStart` and no `QueryStart`, but still logs the query it was sent.
SELECT 'what the replicas were sent: the equality, never the lambda';
SELECT
    log_comment,
    count() > 0 AS sent_a_query,
    countIf(query LIKE '%HAVING%equals(%tenant%') = count() AS all_carry_the_equality,
    countIf(query LIKE '%arrayCount%') > 0 AS some_carry_the_lambda,
    countIf(query LIKE '%HAVING%') > 0 AS some_carry_a_condition
FROM system.query_log
WHERE has(databases, currentDatabase()) AND log_comment IN ('05055_lambda', '05055_beside')
  AND NOT is_initial_query
GROUP BY log_comment ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;

DROP VIEW v_lambda_key_pr_local_pd;
DROP VIEW v_pr_local_pd;
DROP TABLE t_pr_local_pd_lambda_key;
DROP TABLE t_pr_local_pd;
