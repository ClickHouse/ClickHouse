-- A condition pushed into the fragment the initiator runs locally is also spliced into the query the
-- replicas are sent - except for a conjunct the rewrite cannot write back as an AST, and a lambda is
-- one of those (`tryBuildAdditionalFilterAST`). A sorting key may be a higher-order expression, so
-- such a conjunct can be exactly the one that would fix a key column: the initiator would order its
-- read by something the replicas never receive, announcing `InOrder` against their `Default`.
--
-- So the question is asked conjunct by conjunct: a lambda-bearing equality may not order the read,
-- while a plain equality standing next to a lambda still may - that one travels, and withholding the
-- ordering there would leave the replicas the ones reading in order, diverging the other way round.
-- Today read-in-order does not match a higher-order key expression in the first place, so the first
-- check guards the invariant rather than reproducing a failure: no ordering off a condition the
-- replicas do not have, whichever of the two implementations changes.

DROP TABLE IF EXISTS t_pr_lambda_key;
DROP VIEW IF EXISTS v_pr_lambda_key;
DROP TABLE IF EXISTS t_pr_lambda_beside;
DROP VIEW IF EXISTS v_pr_lambda_beside;

CREATE TABLE t_pr_lambda_key (arr Array(UInt64), ts UInt64)
    ENGINE = MergeTree ORDER BY (arrayCount(x -> x = 1, arr), ts) SETTINGS index_granularity = 128;
INSERT INTO t_pr_lambda_key SELECT arrayMap(i -> i % 4, range(number % 8)), number FROM numbers(10000);

CREATE TABLE t_pr_lambda_beside (tenant UInt64, arr Array(UInt64), ts UInt64)
    ENGINE = MergeTree ORDER BY (tenant, ts) SETTINGS index_granularity = 128;
INSERT INTO t_pr_lambda_beside SELECT number % 100, arrayMap(i -> i % 4, range(number % 8)), number FROM numbers(10000);

-- The sort has to be inside the fragment for the read to order itself at all.
CREATE VIEW v_pr_lambda_key AS SELECT arr, ts FROM t_pr_lambda_key ORDER BY ts;
CREATE VIEW v_pr_lambda_beside AS SELECT tenant, arr, ts FROM t_pr_lambda_beside ORDER BY ts;

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
-- Pin what decides whether a condition reaches the replicas: it is spliced into the query they are
-- sent, so neither a plan-based fragment nor a serialized plan carries it.
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;
SET parallel_replicas_plan_based = 0;

SELECT 'a lambda-bearing equality does not order the read';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT arr, ts FROM v_pr_lambda_key WHERE arrayCount(x -> x = 1, arr) = 2 LIMIT 5
)
WHERE explain LIKE '%Read type%';

SELECT 'and answers correctly';
SELECT arrayCount(x -> x = 1, arr), ts FROM v_pr_lambda_key WHERE arrayCount(x -> x = 1, arr) = 2 ORDER BY ts LIMIT 3;

SELECT 'a plain equality next to a lambda still orders it';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (
    EXPLAIN description = 0, actions = 1
    SELECT tenant, ts FROM v_pr_lambda_beside WHERE tenant = 5 AND arrayExists(x -> x = 1, arr) LIMIT 5
)
WHERE explain LIKE '%Read type%';

SELECT 'and answers correctly';
SELECT tenant, ts FROM v_pr_lambda_beside WHERE tenant = 5 AND arrayExists(x -> x = 1, arr) ORDER BY ts LIMIT 3;

-- Both queries again for the queries the replicas are sent, with the local plan off. With one, the
-- initiator's own share covers the table and the replicas are cancelled before they start - their log
-- rows are then written after the initiator has finished, and can miss the flush below. Without one
-- the initiator has to consume their streams to the end, so by the time it returns they are logged.
SELECT arr, ts FROM v_pr_lambda_key WHERE arrayCount(x -> x = 1, arr) = 2 ORDER BY ts
SETTINGS log_comment = '05259_lambda_key', parallel_replicas_local_plan = 0 FORMAT Null;

SELECT tenant, ts FROM v_pr_lambda_beside WHERE tenant = 5 AND arrayExists(x -> x = 1, arr) ORDER BY ts
SETTINGS log_comment = '05259_lambda_beside', parallel_replicas_local_plan = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- A replica query is scoped by the database it reads, not by `current_database`: that one is logged as
-- `default` for it, while `databases` holds this test's. `log_comment` travels with the query to the
-- replicas, which is what separates these queries' replicas from any other. Rows of every type count:
-- a replica cancelled before it started logs `ExceptionBeforeStart` and no `QueryStart`, but it still
-- logs the query it was sent.
SELECT 'the lambda reaches none of them';
SELECT count() > 0 AS replicas_were_sent_a_query, countIf(query LIKE '%arrayCount%') AS carrying_it
FROM system.query_log
WHERE has(databases, currentDatabase()) AND log_comment = '05259_lambda_key' AND NOT is_initial_query
SETTINGS enable_parallel_replicas = 0;

SELECT 'the plain equality standing next to one reaches all of them';
SELECT count() > 0 AS replicas_were_sent_a_query, countIf(query LIKE '%HAVING%equals(%tenant%') = count() AS carrying_it
FROM system.query_log
WHERE has(databases, currentDatabase()) AND log_comment = '05259_lambda_beside' AND NOT is_initial_query
SETTINGS enable_parallel_replicas = 0;

DROP VIEW v_pr_lambda_key;
DROP VIEW v_pr_lambda_beside;
DROP TABLE t_pr_lambda_key;
DROP TABLE t_pr_lambda_beside;
