-- Tags: no-random-settings
-- This test asserts the exact plan shape (the number of reads carrying deferred filters), so
-- settings randomization must be disabled, as in the sibling tests 04402 / 04515.
--
-- Regression for `ReadFromMergeTree::clone` propagating the whole `applyFilters`-derived state
-- (`indexes`, `deferred_row_level_filter`, `deferred_prewhere_info`, `skip_partition_pruning`),
-- not only `index_analysis_had_filter`.
--
-- With `correlated_subqueries_use_in_memory_buffer = 0`, decorrelating a correlated subquery wraps
-- the outer relation in a `CommonSubplanStep` referenced twice, and `materializeQueryPlanReferences`
-- clones the subplan via `ReadFromMergeTree::clone`. `applyFilters` (and with it
-- `deferFiltersAfterFinalIfNeeded`) runs only before the clone and is not run again on it, so a
-- clone that dropped the derived state would apply a deferred row policy / PREWHERE during reading
-- (before `FINAL`, changing which row wins deduplication) and would rebuild index analysis from the
-- raw filter DAG with partition pruning re-enabled.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET correlated_subqueries_use_in_memory_buffer = 0;
SET apply_row_policy_after_final = 1;

DROP TABLE IF EXISTS tab_cloned_final;
DROP ROW POLICY IF EXISTS pol_cloned_final ON tab_cloned_final;

CREATE TABLE tab_cloned_final (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY x;

SYSTEM STOP MERGES tab_cloned_final;

INSERT INTO tab_cloned_final VALUES (1, 'aaa', 1), (2, 'bbb', 1);
INSERT INTO tab_cloned_final VALUES (1, 'ccc', 2);

-- The policy is on a non-sorting-key column, so it must be applied after FINAL.
CREATE ROW POLICY pol_cloned_final ON tab_cloned_final USING y != 'ccc' TO ALL;

-- The correlated EXISTS forces decorrelation: the outer `FINAL` read becomes a common subplan
-- referenced twice, the second reference being a clone. Both reads must carry the deferred row
-- policy, so the plan must show it exactly twice.
SELECT 'deferred_row_policy_on_both_reads';
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT x, y FROM tab_cloned_final FINAL
    WHERE EXISTS (SELECT 1 FROM numbers(10) WHERE number != tab_cloned_final.version)
    ORDER BY x
) WHERE explain LIKE '%Deferred filters (applied after FINAL)%';

-- Result correctness: FINAL picks (1, 'ccc', 2) as the winner for x = 1, and only then does the
-- row policy hide it; the stale (1, 'aaa', 1) must not resurface through the cloned read.
SELECT 'results';
SELECT x, y FROM tab_cloned_final FINAL
WHERE EXISTS (SELECT 1 FROM numbers(10) WHERE number != tab_cloned_final.version)
ORDER BY x;

-- Same for a deferred PREWHERE.
SELECT 'deferred_prewhere_on_both_reads';
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT x, y FROM tab_cloned_final FINAL
    PREWHERE y != 'ccc'
    WHERE EXISTS (SELECT 1 FROM numbers(10) WHERE number != tab_cloned_final.version)
    ORDER BY x
    SETTINGS apply_prewhere_after_final = 1
) WHERE explain LIKE '%Deferred prewhere filter column%';

DROP ROW POLICY pol_cloned_final ON tab_cloned_final;

SELECT 'results_prewhere';
SELECT x, y FROM tab_cloned_final FINAL
PREWHERE y != 'ccc'
WHERE EXISTS (SELECT 1 FROM numbers(10) WHERE number != tab_cloned_final.version)
ORDER BY x
SETTINGS apply_prewhere_after_final = 1;

DROP TABLE tab_cloned_final;
