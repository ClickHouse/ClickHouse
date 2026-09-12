-- A row policy with a non-deterministic lambda folded into a constant ColumnFunction
-- must be deferred after FINAL, otherwise stale row versions can win the merge
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET apply_row_policy_after_final = 1;

DROP TABLE IF EXISTS t_rp_folded_lambda;

CREATE TABLE t_rp_folded_lambda (k Int32, data String, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k;

INSERT INTO t_rp_folded_lambda SELECT number, 'old', 1 FROM numbers(10000);
INSERT INTO t_rp_folded_lambda SELECT number, 'new', 2 FROM numbers(10000);

DROP ROW POLICY IF EXISTS pol_rp_folded_lambda ON t_rp_folded_lambda;
CREATE ROW POLICY pol_rp_folded_lambda ON t_rp_folded_lambda USING arrayExists(x -> rand(x) % 2 = 0, [k]) TO ALL;

SELECT '= the policy is deferred after FINAL =';
SELECT count() FROM (EXPLAIN actions=1 SELECT k FROM t_rp_folded_lambda FINAL) WHERE explain LIKE '%Deferred row level filter%';

-- stale versions must never be visible, whatever the policy verdicts are
SELECT '= stale rows never leak =';
SELECT countIf(data = 'old') FROM t_rp_folded_lambda FINAL;

DROP ROW POLICY pol_rp_folded_lambda ON t_rp_folded_lambda;
DROP TABLE t_rp_folded_lambda;
