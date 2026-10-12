-- Whether the replicas get a condition pushed into the fragment is answered once for the whole
-- fragment, not per conjunct, and the answer decides whether the initiator may order its read by that
-- condition. So the initiator can order a read off a condition while the splice carries only part of
-- that condition to the replicas - safe for one reason only: the part that can fix a column is the
-- part the splice never drops.
--
--   * `appendFixedColumnsFromFilterExpression` fixes a column only from an `equals` against a
--     constant, reached through `and` and aliases - not from an `or`, an `in`, or anything else;
--   * `tryBuildAdditionalFilterAST` drops the non-deterministic, the stateful and what it cannot
--     name - a constant is none of those, so such an `equals` always travels.
--
-- The two lists do not meet, which is why one answer per fragment is enough. This test pins the
-- consequence rather than the argument: the read below orders itself off `tenant = 5`, so the query
-- the replicas were sent must contain `tenant = 5`. Should the rewrite ever start dropping such a
-- conjunct - or read-in-order start fixing a column from something the rewrite can drop, an `in`
-- against a one-element set, say - the initiator would read `InOrder` while the replicas read
-- `Default`, and the read would fail with "Replica N decided to read in X mode, not in Y".
-- https://github.com/ClickHouse/ClickHouse/issues/95524

DROP TABLE IF EXISTS t_pr_partial_splice;
DROP VIEW IF EXISTS v_pr_partial_splice;

CREATE TABLE t_pr_partial_splice (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO t_pr_partial_splice SELECT number % 100, number FROM numbers(10000);

-- The sort has to be inside the fragment for the read to order itself at all.
CREATE VIEW v_pr_partial_splice AS SELECT tenant, ts FROM t_pr_partial_splice ORDER BY ts;

SET enable_analyzer = 1;
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
-- Pin what decides whether the condition reaches the replicas: it is spliced into the query they are
-- sent, so neither a plan-based fragment nor a serialized plan carries it.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;
SET parallel_replicas_plan_based = 0;

SELECT 'the read orders itself off the condition';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM v_pr_partial_splice WHERE tenant = 5 AND ts < 100000 LIMIT 5
)
WHERE explain LIKE '%Read type%';

SELECT 'and answers correctly';
SELECT tenant, ts FROM v_pr_partial_splice WHERE tenant = 5 AND ts < 100000 ORDER BY ts LIMIT 3;

-- The same query again for the query the replicas are sent, with the local plan off. With one, the
-- initiator's own share covers the table and the replicas are cancelled before they start - their log
-- rows are then written after the initiator has finished, and can miss the flush below. Without one
-- the initiator has to consume their streams to the end, so by the time it returns they are logged.
-- The splice does not depend on there being a local plan: it is the remote step that performs it.
SELECT tenant, ts FROM v_pr_partial_splice WHERE tenant = 5 AND ts < 100000 ORDER BY ts
SETTINGS log_comment = '05255_pr_partial_splice', parallel_replicas_local_plan = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'the replicas were sent that same condition';
-- A replica query is scoped by the database it reads, not by `current_database`: that one is logged
-- as `default` for it, while `databases` holds this test's. `log_comment` travels with the query to
-- the replicas, which is what separates this query's replicas from any other. Rows of every type
-- count: the initiator cancels a replica once it has the rows it needs, and one cancelled before it
-- started logs `ExceptionBeforeStart` and no `QueryStart` - but it still logs the query it was sent,
-- which is the whole of what is checked here.
SELECT
    count() > 0 AS replicas_were_sent_a_query,
    countIf(query LIKE '%HAVING%equals(%tenant%') = count() AS every_one_got_the_condition
FROM system.query_log
WHERE has(databases, currentDatabase()) AND log_comment = '05255_pr_partial_splice'
  AND NOT is_initial_query
SETTINGS enable_parallel_replicas = 0;

DROP VIEW v_pr_partial_splice;
DROP TABLE t_pr_partial_splice;
