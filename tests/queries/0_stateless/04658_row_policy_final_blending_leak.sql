-- FINAL on an engine which merges rows must not show values of the rows a policy hides

SET allow_suspicious_row_policies_with_blending_engines = 1;

CREATE TABLE t_coalescing (key String, data1 Nullable(String), data2 Nullable(String))
ENGINE = CoalescingMergeTree ORDER BY key;

SYSTEM STOP MERGES t_coalescing;
INSERT INTO t_coalescing VALUES ('key', 'sensitive_data', 'top_secret');
INSERT INTO t_coalescing VALUES ('key', 'not sensitive data', NULL);

CREATE ROW POLICY p_coalescing ON t_coalescing USING data1 != 'sensitive_data' TO ALL;

SELECT 'coalescing', * FROM t_coalescing FINAL;
SELECT 'coalescing', * FROM t_coalescing FINAL SETTINGS apply_row_policy_after_final = 0;

CREATE TABLE t_summing (key String, secret UInt64) ENGINE = SummingMergeTree ORDER BY key;

SYSTEM STOP MERGES t_summing;
INSERT INTO t_summing VALUES ('key', 1000000);
INSERT INTO t_summing VALUES ('key', 1);

CREATE ROW POLICY p_summing ON t_summing USING secret < 100 TO ALL;

SELECT 'summing', * FROM t_summing FINAL;

-- soft deletes still need the policy after FINAL, that part must not change
CREATE TABLE t_replacing (key String, deleted UInt8, data String) ENGINE = ReplacingMergeTree ORDER BY key;

SYSTEM STOP MERGES t_replacing;
INSERT INTO t_replacing VALUES ('key', 0, 'old');
INSERT INTO t_replacing VALUES ('key', 1, 'new');

CREATE ROW POLICY p_replacing ON t_replacing USING deleted = 0 TO ALL;

SELECT 'replacing', count() FROM t_replacing FINAL;

DROP ROW POLICY p_coalescing ON t_coalescing;
DROP ROW POLICY p_summing ON t_summing;
DROP ROW POLICY p_replacing ON t_replacing;
