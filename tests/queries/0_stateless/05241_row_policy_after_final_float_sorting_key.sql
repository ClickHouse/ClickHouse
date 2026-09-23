-- A row policy over a float sorting key column must be deferred after FINAL:
-- -0.0 and 0.0 land in one dedup group but toString tells them apart
SET explain_query_plan_default = 'legacy';
SET apply_row_policy_after_final = 1;

DROP TABLE IF EXISTS t_rp_float_key;

CREATE TABLE t_rp_float_key (id UInt64, f Float64, v UInt64,
    INDEX ix_f toString(f) TYPE set(0) GRANULARITY 1)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, f);

INSERT INTO t_rp_float_key VALUES (1, 0.0, 1);
INSERT INTO t_rp_float_key VALUES (1, -0.0, 2);

DROP ROW POLICY IF EXISTS pol_rp_float_key ON t_rp_float_key;
CREATE ROW POLICY pol_rp_float_key ON t_rp_float_key USING toString(f) = '0' TO ALL;

SELECT '= the policy is deferred after FINAL =';
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_rp_float_key FINAL) WHERE explain LIKE '%Deferred row level filter%';

-- the winner is (1, -0.0, 2) and the policy hides it, the stale (1, 0.0, 1) must not reappear
SELECT '= the stale version stays hidden =';
SELECT id, f, v FROM t_rp_float_key FINAL;
SELECT id, f, v FROM t_rp_float_key FINAL SETTINGS use_skip_indexes_if_final = 1;

DROP ROW POLICY pol_rp_float_key ON t_rp_float_key;
DROP TABLE t_rp_float_key;
