-- Automatic parallel replicas decides whether replicas pay off by building a second, candidate plan
-- and costing it. For a query parallel replicas cannot read at all, that plan is built only to be
-- recognized as useless and thrown away, so the eligibility of the query is checked before building
-- it. `AutomaticParallelReplicasProbePlansBuilt` counts the candidate plans actually built, so it is
-- non-zero exactly for the queries that reach the check and pass it.
--
-- The eligible cases are the ones worth pinning. Eligibility depends on settings the query's own
-- SETTINGS clause can still change, and that clause is applied while building the query tree, not to
-- the context the check is handed. A regression that reads those settings from the wrong context
-- would reject such queries, and parallel replicas would silently stop being considered for them.

DROP TABLE IF EXISTS t_autopr_gate;
DROP TABLE IF EXISTS t_autopr_gate_2;

-- ReplacingMergeTree so that the FINAL case below is a legal query; plain MergeTree rejects FINAL
-- outright, and eligibility is otherwise identical (both are non-replicated MergeTree family).
CREATE TABLE t_autopr_gate (a UInt64, b UInt64) ENGINE = ReplacingMergeTree ORDER BY a;
CREATE TABLE t_autopr_gate_2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_autopr_gate SELECT number, number % 100 FROM numbers(10000);
INSERT INTO t_autopr_gate_2 SELECT number, number % 10 FROM numbers(1000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET max_parallel_replicas = 3;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
-- The byte pre-gate rejects a read this small before the eligibility check is reached; 0 disables it
-- so that every query below actually exercises the check.
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
-- Without a local plan no candidate plan is built at all, and the plan-based implementation decides
-- eligibility from the plan rather than from the query tree. Either would make the counts vacuous.
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_plan_based = 0;

-- Eligible: a plain aggregate over one MergeTree table.
SELECT b, count() FROM t_autopr_gate GROUP BY b FORMAT Null SETTINGS log_comment = 'autopr_gate_eligible_plain';

-- Ineligible: a FULL JOIN cannot be evaluated by parallelizing one side.
SELECT count() FROM t_autopr_gate AS l FULL JOIN t_autopr_gate_2 AS r ON l.a = r.a FORMAT Null
SETTINGS query_plan_optimize_join_order_randomize = 0, log_comment = 'autopr_gate_ineligible_full_join';

-- Ineligible: only one replica may be used.
SELECT b, count() FROM t_autopr_gate GROUP BY b FORMAT Null
SETTINGS max_parallel_replicas = 1, log_comment = 'autopr_gate_ineligible_one_replica';

-- Ineligible: parallel replicas are not allowed on non-replicated MergeTree.
SELECT b, count() FROM t_autopr_gate GROUP BY b FORMAT Null
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, log_comment = 'autopr_gate_ineligible_plain_merge_tree';

-- Never considered: the query's own `SETTINGS enable_parallel_replicas = 0` is applied to the query
-- context before the interpreter runs (`InterpreterSetQuery::applySettingsFromQuery`), so
-- `buildContext` switches `automatic_parallel_replicas_mode` off and the candidate plan is never
-- requested. The per-query opt-out is honored above the eligibility check, not by it.
SELECT b, count() FROM t_autopr_gate GROUP BY b FORMAT Null
SETTINGS enable_parallel_replicas = 0, log_comment = 'autopr_gate_ineligible_query_optout';

-- Never considered: FINAL is rejected before the eligibility check, by the plan itself.
SELECT b, count() FROM t_autopr_gate FINAL GROUP BY b FORMAT Null
SETTINGS log_comment = 'autopr_gate_ineligible_final';

-- Eligible only through a subquery's own SETTINGS clause, which allows parallel replicas on
-- non-replicated MergeTree again after the outer query forbade them. The subquery is planned with its
-- own context and the read below it is made with replicas, so a check that only consulted the outer
-- context would skip the candidate plan. `sum` rather than `count` so that the query reads a column.
SELECT sum(a) FROM (SELECT a FROM t_autopr_gate SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1)
FORMAT Null
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0, log_comment = 'autopr_gate_eligible_by_nested_settings';

-- Eligible only because of the query's own SETTINGS clause, which raises the replica count back above
-- one. Reading the session setting instead would reject this query.
SET max_parallel_replicas = 1;
SELECT b, count() FROM t_autopr_gate GROUP BY b FORMAT Null
SETTINGS max_parallel_replicas = 3, log_comment = 'autopr_gate_eligible_by_query_settings';
SET max_parallel_replicas = 3;

-- Eligible for the same reason through a different setting: the session forbids parallel replicas on
-- non-replicated MergeTree and the query allows them again.
SET parallel_replicas_for_non_replicated_merge_tree = 0;
SELECT b, count() FROM t_autopr_gate GROUP BY b FORMAT Null
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 1, log_comment = 'autopr_gate_eligible_by_query_settings_2';
SET parallel_replicas_for_non_replicated_merge_tree = 1;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] > 0 AS candidate_plan_built
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND startsWith(log_comment, 'autopr_gate_')
ORDER BY log_comment;

DROP TABLE t_autopr_gate;
DROP TABLE t_autopr_gate_2;
