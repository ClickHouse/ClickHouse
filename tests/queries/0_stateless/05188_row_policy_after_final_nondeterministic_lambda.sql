-- A row policy whose condition hides a non-deterministic call inside a lambda body must stay deferred
-- until after `FINAL`: applied before it, the policy is evaluated for every row of a dedup group, and
-- two draws that disagree flip the row that wins the group. The lambda has no non-constant captured
-- columns, so it is folded into a constant `ColumnFunction` and is not a `FUNCTION` node of the
-- row-policy DAG, which is what the determinism gate has to see through.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET apply_row_policy_after_final = 1;

DROP TABLE IF EXISTS tab_nondet_lambda_policy;
DROP ROW POLICY IF EXISTS pol_nondet_lambda ON tab_nondet_lambda_policy;
DROP ROW POLICY IF EXISTS pol_det_lambda ON tab_nondet_lambda_policy;

CREATE TABLE tab_nondet_lambda_policy (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO tab_nondet_lambda_policy VALUES (1, 'aaa', 1), (2, 'bbb', 1);

-- The condition reads only `x`, a sorting key column, so the only thing that can defer it is the
-- non-determinism inside the lambda.
CREATE ROW POLICY pol_nondet_lambda ON tab_nondet_lambda_policy USING arrayExists(z -> rand(z) % 2 = 0, [x]) TO ALL;

SELECT '--- a non-deterministic lambda in the row policy stays deferred';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT * FROM tab_nondet_lambda_policy FINAL ORDER BY x)
WHERE explain LIKE '%Deferred row level filter%';

DROP ROW POLICY pol_nondet_lambda ON tab_nondet_lambda_policy;

-- Control: the same shape with a deterministic body is still applied before `FINAL`, so the gate did
-- not degrade into deferring every lambda.
CREATE ROW POLICY pol_det_lambda ON tab_nondet_lambda_policy USING arrayExists(z -> z > 0, [x]) TO ALL;

SELECT '--- a deterministic lambda over the sorting key is not deferred';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT * FROM tab_nondet_lambda_policy FINAL ORDER BY x)
WHERE explain LIKE '%Deferred%';

SELECT '--- data correctness';
SELECT * FROM tab_nondet_lambda_policy FINAL ORDER BY x;

DROP ROW POLICY pol_det_lambda ON tab_nondet_lambda_policy;
DROP TABLE tab_nondet_lambda_policy;
