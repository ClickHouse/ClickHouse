-- A lambda whose captures are all constant is folded into a `COLUMN` node holding a `ColumnFunction`, and the
-- call that takes the lambda argument lives in that lambda's own `ActionsDAG`. A walk over the filter DAG that
-- only inspects `FUNCTION` nodes therefore does not see it, and `apply_row_policy_after_final` used to classify
-- such a row policy as deterministic and evaluate it before `FINAL`. Evaluating a non-deterministic policy
-- before dedup can keep a version that would have disappeared had the policy been evaluated after `FINAL`.
--
-- `randConstant` is the interesting body here: it is not deterministic, but it is deterministic in scope of the
-- query, so it passes the constant-folding gate and the folded carrier really is built.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET apply_row_policy_after_final = 1;

DROP TABLE IF EXISTS t_row_policy_lambda;
DROP ROW POLICY IF EXISTS p_row_policy_lambda ON t_row_policy_lambda;

CREATE TABLE t_row_policy_lambda (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO t_row_policy_lambda VALUES (1, 'aaa', 1), (2, 'bbb', 1);

-- Structurally over the sorting key only, but the body of the lambda is not deterministic, so the policy must
-- stay deferred until after `FINAL`.
CREATE ROW POLICY p_row_policy_lambda ON t_row_policy_lambda
USING arrayExists(z -> randConstant(z) % 2 = 0, [x]) TO ALL;

SELECT 'non-deterministic lambda row policy stays deferred';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT * FROM t_row_policy_lambda FINAL ORDER BY x)
WHERE explain LIKE '%Deferred row level filter%';

DROP ROW POLICY p_row_policy_lambda ON t_row_policy_lambda;

-- Control: the same shape with a deterministic body is sorting-key-only and safe before `FINAL`.
CREATE ROW POLICY p_row_policy_lambda ON t_row_policy_lambda
USING arrayExists(z -> z % 2 = 0, [x]) TO ALL;

SELECT 'deterministic lambda row policy is not deferred';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT * FROM t_row_policy_lambda FINAL ORDER BY x)
WHERE explain LIKE '%Deferred row level filter%';

DROP ROW POLICY p_row_policy_lambda ON t_row_policy_lambda;
DROP TABLE t_row_policy_lambda;
