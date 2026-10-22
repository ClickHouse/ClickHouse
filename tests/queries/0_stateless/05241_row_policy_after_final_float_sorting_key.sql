-- -0.0 and 0.0 merge as one key, so a policy on a float sorting key must be deferred after FINAL
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

-- the policy hides the winner (1, -0.0, 2), the stale (1, 0.0, 1) must not reappear
SELECT '= the stale version stays hidden =';
SELECT id, f, v FROM t_rp_float_key FINAL;

-- this policy defers because of v, so its float atom reaches index analysis and must be dropped there
SELECT '= float atoms are excluded from index analysis =';
DROP ROW POLICY pol_rp_float_key ON t_rp_float_key;
CREATE ROW POLICY pol_rp_float_key ON t_rp_float_key USING v >= 0 AND toString(f) = '0' TO ALL;
SELECT id, f, v FROM t_rp_float_key FINAL SETTINGS use_skip_indexes_if_final = 1, use_skip_indexes_if_final_exact_mode = 0;

DROP ROW POLICY pol_rp_float_key ON t_rp_float_key;
DROP TABLE t_rp_float_key;

SELECT '= NaN payloads dedup as one key =';
DROP TABLE IF EXISTS t_rp_nan_key;
CREATE TABLE t_rp_nan_key (id UInt64, f Float64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY (id, f);
INSERT INTO t_rp_nan_key VALUES (1, reinterpretAsFloat64(toUInt64(9221120237041090560)), 1);
INSERT INTO t_rp_nan_key VALUES (1, reinterpretAsFloat64(toUInt64(9221120237041090561)), 2);
DROP ROW POLICY IF EXISTS pol_rp_nan_key ON t_rp_nan_key;
CREATE ROW POLICY pol_rp_nan_key ON t_rp_nan_key USING reinterpretAsUInt64(f) = 9221120237041090560 TO ALL;
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_rp_nan_key FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT id, reinterpretAsUInt64(f), v FROM t_rp_nan_key FINAL;
DROP ROW POLICY pol_rp_nan_key ON t_rp_nan_key;
DROP TABLE t_rp_nan_key;

SELECT '= nullable float sorting key =';
DROP TABLE IF EXISTS t_rp_null_key;
CREATE TABLE t_rp_null_key (id UInt64, f Nullable(Float64), v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, f) SETTINGS allow_nullable_key = 1;
INSERT INTO t_rp_null_key VALUES (1, 0.0, 1);
INSERT INTO t_rp_null_key VALUES (1, -0.0, 2);
DROP ROW POLICY IF EXISTS pol_rp_null_key ON t_rp_null_key;
CREATE ROW POLICY pol_rp_null_key ON t_rp_null_key USING toString(f) = '0' TO ALL;
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_rp_null_key FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT id, f, v FROM t_rp_null_key FINAL;
DROP ROW POLICY pol_rp_null_key ON t_rp_null_key;
DROP TABLE t_rp_null_key;

SELECT '= tuple with float in sorting key =';
DROP TABLE IF EXISTS t_rp_tuple_key;
CREATE TABLE t_rp_tuple_key (id UInt64, t Tuple(Float64, UInt8), v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, t);
INSERT INTO t_rp_tuple_key VALUES (1, (0.0, 1), 1);
INSERT INTO t_rp_tuple_key VALUES (1, (-0.0, 1), 2);
DROP ROW POLICY IF EXISTS pol_rp_tuple_key ON t_rp_tuple_key;
CREATE ROW POLICY pol_rp_tuple_key ON t_rp_tuple_key USING toString(t) = '(0,1)' TO ALL;
SELECT count() FROM (EXPLAIN actions=1 SELECT id FROM t_rp_tuple_key FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT id, t, v FROM t_rp_tuple_key FINAL;
DROP ROW POLICY pol_rp_tuple_key ON t_rp_tuple_key;
DROP TABLE t_rp_tuple_key;
