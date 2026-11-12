-- Tags: no-parallel, no-fasttest
-- A fail point armed here fires for every query on the server, so this test must run alone.

-- Where the replicas do not get a condition that could fix a sort key column, it must stay above the
-- initiator's copy of the fragment, or that copy reads in order while the replicas do not: "Got read
-- request from replica N for unknown stream". Two conditions whose effect only shows inside the fragment:
-- a bare boolean that the fragment computes as `tenant = 5`, and `tenant = 5` itself where the fragment
-- fixes only `tenant % 2`. The fail points make the replicas take part before the initiator's own read
-- has claimed every range.

DROP TABLE IF EXISTS t_pr_computed_output;
DROP VIEW IF EXISTS v_pr_computed_output;
DROP VIEW IF EXISTS v_odd_pr_computed_output;

CREATE TABLE t_pr_computed_output (tenant UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (tenant, ts)
    SETTINGS index_granularity = 128;
INSERT INTO t_pr_computed_output SELECT number % 100, number FROM numbers(200000);

CREATE VIEW v_pr_computed_output AS SELECT tenant, ts, tenant = 5 AS is_five FROM t_pr_computed_output ORDER BY ts;
CREATE VIEW v_odd_pr_computed_output AS SELECT tenant, ts FROM t_pr_computed_output WHERE tenant % 2 = 1 ORDER BY ts;

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET parallel_replicas_allow_view_over_mergetree = 1;
SET parallel_replicas_plan_based = 0;
SET parallel_replicas_mark_segment_size = 8;
SET optimize_read_in_order = 1;

SELECT 'no splice: the condition stays above, nothing is ordered';
SET allow_push_predicate_ast_for_distributed_subqueries = 0;
SET serialize_query_plan = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_pr_computed_output WHERE is_five ORDER BY ts)
WHERE explain LIKE '%Read type%';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT count() FROM (SELECT tenant, ts FROM v_pr_computed_output WHERE is_five ORDER BY ts);
SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_odd_pr_computed_output WHERE tenant = 5 ORDER BY ts)
WHERE explain LIKE '%Read type%';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT count() FROM (SELECT tenant, ts FROM v_odd_pr_computed_output WHERE tenant = 5 ORDER BY ts);
SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT 'a fragment shipped as a plan: the same';
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_pr_computed_output WHERE is_five ORDER BY ts)
WHERE explain LIKE '%Read type%';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT count() FROM (SELECT tenant, ts FROM v_pr_computed_output WHERE is_five ORDER BY ts);
SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT 'spliced, the replicas fix the column too, and both sides read in order';
SET serialize_query_plan = 0;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') AS step
FROM (EXPLAIN description = 0, actions = 1 SELECT tenant, ts FROM v_pr_computed_output WHERE is_five ORDER BY ts)
WHERE explain LIKE '%Read type%';
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT count() FROM (SELECT tenant, ts FROM v_pr_computed_output WHERE is_five ORDER BY ts);
SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SYSTEM DISABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

DROP VIEW v_odd_pr_computed_output;
DROP VIEW v_pr_computed_output;
DROP TABLE t_pr_computed_output;
