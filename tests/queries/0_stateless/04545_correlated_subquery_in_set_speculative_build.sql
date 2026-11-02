-- Regression test for issue #110182.
-- When an IN (subquery) set is speculatively built during index analysis
-- (KeyCondition::tryPrepareSetIndexForIn -> FutureSetFromSubquery::buildOrderedSetInplace),
-- the set body plan is cloned. CommonSubplanReferenceStep::clone used to keep the raw
-- subplan_root pointer into the original plan, so optimizing the clone rewrote a node of the
-- original plan (the in-memory buffer optimization replaces the referenced node's step with
-- SaveSubqueryResultToBufferStep). Any later optimization of a plan referencing that node threw
--   Logical error: Expected CommonSubplanReferenceStep to reference CommonSubplanStep
-- and the consumer ReadFromCommonBufferStep ended up in a different pipeline than its producer.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET correlated_subqueries_use_in_memory_buffer = 1;

DROP TABLE IF EXISTS t_subplan_ref_clone;

CREATE TABLE t_subplan_ref_clone (dt DateTime, idx Int32, i Nullable(UInt64)) ENGINE = MergeTree PARTITION BY dt ORDER BY idx;

INSERT INTO t_subplan_ref_clone SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL number % 3 DAY, number, if(number % 5 = 0, NULL, number) FROM numbers(1000);

-- A plain IN on the primary key column: index analysis builds the set from a clone of the set
-- body plan, and the body carries a decorrelated correlated scalar subquery
-- (CommonSubplanStep + CommonSubplanReferenceStep).
SELECT count() FROM t_subplan_ref_clone WHERE idx IN (SELECT idx FROM t_subplan_ref_clone WHERE (SELECT dt) > 256);

-- The same with the correlated predicate selecting nothing.
SELECT count() FROM t_subplan_ref_clone WHERE idx IN (SELECT idx FROM t_subplan_ref_clone WHERE (SELECT dt) < 256);

-- Decorrelation pins the layout of its result join (JoinStepLogical::setOptimized): only the
-- writer-on-the-build-side layout guarantees that the in-memory buffer of the common subplan is
-- fully written before ReadFromCommonBufferSource reads it. JoinStepLogical::clone used to drop
-- the `optimized` flag, so re-optimizing the speculatively cloned plan could swap the join sides
-- (ANY strictness allows a swap) and schedule the buffer reader before the writers, failing with
--   Logical error: Trying to extract chunk from ChunkBuffer before all inputs are finished.
-- Randomized statistics make the swap deterministic for some seeds (e.g. 1, 2, 3, 6 at the time
-- of writing); sweep a few so at least one exercises the swapped layout.
SELECT count() FROM t_subplan_ref_clone WHERE idx IN (SELECT idx FROM t_subplan_ref_clone WHERE (SELECT dt) > 256) SETTINGS query_plan_optimize_join_order_randomize = 1;
SELECT count() FROM t_subplan_ref_clone WHERE idx IN (SELECT idx FROM t_subplan_ref_clone WHERE (SELECT dt) > 256) SETTINGS query_plan_optimize_join_order_randomize = 2;
SELECT count() FROM t_subplan_ref_clone WHERE idx IN (SELECT idx FROM t_subplan_ref_clone WHERE (SELECT dt) > 256) SETTINGS query_plan_optimize_join_order_randomize = 3;
SELECT count() FROM t_subplan_ref_clone WHERE idx IN (SELECT idx FROM t_subplan_ref_clone WHERE (SELECT dt) > 256) SETTINGS query_plan_optimize_join_order_randomize = 6;

-- The trigger reported in issue #110182 (found by AST fuzzer): self-referential subqueries in
-- PREWHERE and WHERE with a nested correlated scalar, built inplace during MergeTree part pruning.
SELECT DISTINCT 3, count() <= -2147483648
FROM t_subplan_ref_clone
PREWHERE i GLOBAL NOT IN (SELECT i FROM t_subplan_ref_clone WHERE dt <= -1)
WHERE i GLOBAL IN (SELECT '\0', i FROM t_subplan_ref_clone PREWHERE dt < -2147483648 WHERE (SELECT dt) < 256);

DROP TABLE t_subplan_ref_clone;
