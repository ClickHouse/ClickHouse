-- When PREWHERE is applied after FINAL, moving WHERE conditions to PREWHERE must be skipped,
-- otherwise they escape the deferral and run before a deferred row policy
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere_if_final = 1;
SET apply_row_policy_after_final = 1;

DROP TABLE IF EXISTS t_prewhere_final_defer;

CREATE TABLE t_prewhere_final_defer (k Int32, cat String, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k;

INSERT INTO t_prewhere_final_defer VALUES (1, 'a', 1), (2, 'b', 1);

SELECT '= apply_prewhere_after_final = 1: WHERE is not moved =';
SELECT count() FROM (EXPLAIN actions=1 SELECT * FROM t_prewhere_final_defer FINAL WHERE k >= 1 SETTINGS apply_prewhere_after_final = 1) WHERE explain LIKE '%Prewhere filter%';

SELECT '= deferred row policy: WHERE is not moved and runs after the policy =';
DROP ROW POLICY IF EXISTS pol_prewhere_final_defer ON t_prewhere_final_defer;
CREATE ROW POLICY pol_prewhere_final_defer ON t_prewhere_final_defer USING cat != 'b' TO ALL;

SELECT count() FROM (EXPLAIN actions=1 SELECT k, cat FROM t_prewhere_final_defer FINAL WHERE k >= 1) WHERE explain LIKE '%Prewhere filter%';

-- if moved, the condition would run on the policy-hidden row k = 2 and throw ILLEGAL_DIVISION
SELECT k FROM t_prewhere_final_defer FINAL WHERE intDiv(1, k - 2) <= 0;

SELECT '= sorting-key-only row policy is not deferred, so the move happens =';
DROP ROW POLICY pol_prewhere_final_defer ON t_prewhere_final_defer;
CREATE ROW POLICY pol_prewhere_final_defer ON t_prewhere_final_defer USING k != 100 TO ALL;

SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT k, cat FROM t_prewhere_final_defer FINAL WHERE k >= 1) WHERE explain LIKE '%Prewhere filter%';

DROP ROW POLICY pol_prewhere_final_defer ON t_prewhere_final_defer;
DROP TABLE t_prewhere_final_defer;
