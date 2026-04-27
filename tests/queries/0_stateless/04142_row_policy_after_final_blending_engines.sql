-- Tags: no-parallel
-- Verify that `apply_row_policy_after_final` is not applied for *MergeTree engines
-- whose FINAL blends column values across multiple source rows. Deferring the row
-- policy on such engines used to leak sensitive column values from rows the policy
-- was supposed to hide (see "[Security] apply_row_policy_after_final=true leaks
-- sensitive column values"). The expected behaviour is identical regardless of the
-- `apply_row_policy_after_final` setting on these engines.

SET apply_row_policy_after_final = 1;

SELECT '== CoalescingMergeTree ==';
DROP TABLE IF EXISTS t_coalescing;
DROP ROW POLICY IF EXISTS p_coalescing ON t_coalescing;

CREATE TABLE t_coalescing
(
    key   String,
    data1 Nullable(String),
    data2 Nullable(String)
) ENGINE = CoalescingMergeTree ORDER BY key;

INSERT INTO t_coalescing VALUES ('key', 'sensitive_data', 'top_secret');
INSERT INTO t_coalescing VALUES ('key', 'not sensitive data', NULL);

CREATE ROW POLICY p_coalescing ON t_coalescing USING NOT (data1 = 'sensitive_data') TO ALL;

-- Must never expose 'top_secret' regardless of the setting.
SELECT '-- apply_row_policy_after_final = 1 --';
SELECT * FROM t_coalescing FINAL SETTINGS apply_row_policy_after_final = 1;
SELECT '-- apply_row_policy_after_final = 0 --';
SELECT * FROM t_coalescing FINAL SETTINGS apply_row_policy_after_final = 0;

DROP ROW POLICY p_coalescing ON t_coalescing;
DROP TABLE t_coalescing;


SELECT '== AggregatingMergeTree (anyLast) ==';
DROP TABLE IF EXISTS t_agg;
DROP ROW POLICY IF EXISTS p_agg ON t_agg;

CREATE TABLE t_agg
(
    key   String,
    data1 SimpleAggregateFunction(anyLast, String),
    data2 SimpleAggregateFunction(anyLast, Nullable(String))
) ENGINE = AggregatingMergeTree ORDER BY key;

INSERT INTO t_agg VALUES ('key', 'sensitive_data',     'top_secret');
INSERT INTO t_agg VALUES ('key', 'not sensitive data', NULL);

CREATE ROW POLICY p_agg ON t_agg USING NOT (data1 = 'sensitive_data') TO ALL;

SELECT '-- apply_row_policy_after_final = 1 --';
SELECT * FROM t_agg FINAL SETTINGS apply_row_policy_after_final = 1;
SELECT '-- apply_row_policy_after_final = 0 --';
SELECT * FROM t_agg FINAL SETTINGS apply_row_policy_after_final = 0;

DROP ROW POLICY p_agg ON t_agg;
DROP TABLE t_agg;


SELECT '== SummingMergeTree ==';
DROP TABLE IF EXISTS t_sum;
DROP ROW POLICY IF EXISTS p_sum ON t_sum;

-- Both rows fall into the same dedup group (`key`); the non-summed `visibility`
-- column takes its value from the first row, so a policy filtering on
-- `visibility = 'public'` would pass on the merged row when the first row is
-- public — while `cnt` would still include the contribution of the hidden row.
CREATE TABLE t_sum
(
    key        String,
    visibility String,
    cnt        UInt64
) ENGINE = SummingMergeTree(cnt) ORDER BY key;

INSERT INTO t_sum VALUES ('k', 'public',  5);
INSERT INTO t_sum VALUES ('k', 'private', 100);

CREATE ROW POLICY p_sum ON t_sum USING visibility = 'public' TO ALL;

-- The visible `cnt` must not include the contribution of the private row.
SELECT '-- apply_row_policy_after_final = 1 --';
SELECT * FROM t_sum FINAL SETTINGS apply_row_policy_after_final = 1;
SELECT '-- apply_row_policy_after_final = 0 --';
SELECT * FROM t_sum FINAL SETTINGS apply_row_policy_after_final = 0;

DROP ROW POLICY p_sum ON t_sum;
DROP TABLE t_sum;


SELECT '== ReplacingMergeTree (deferral still works) ==';
-- Sanity check: for engines where deferral is safe, the original
-- soft-delete semantics introduced by `apply_row_policy_after_final` are kept.
DROP TABLE IF EXISTS t_repl;
DROP ROW POLICY IF EXISTS p_repl ON t_repl;

CREATE TABLE t_repl
(
    a UInt64,
    b UInt64,
    is_deleted UInt8,
    version UInt64
) ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY a;

INSERT INTO t_repl VALUES (1, 1, 0, 1);
INSERT INTO t_repl VALUES (1, 1, 1, 2);
INSERT INTO t_repl VALUES (2, 2, 0, 1);

CREATE ROW POLICY p_repl ON t_repl AS RESTRICTIVE FOR SELECT USING (is_deleted = 0) TO ALL;

-- With deferral the soft-deleted row (a = 1) is correctly hidden, only (2, 2) survives.
SELECT '-- apply_row_policy_after_final = 1 --';
SELECT a, b FROM t_repl FINAL ORDER BY a SETTINGS apply_row_policy_after_final = 1;
-- Without deferral, the delete-marker is filtered out before FINAL and (1, 1) reappears.
SELECT '-- apply_row_policy_after_final = 0 --';
SELECT a, b FROM t_repl FINAL ORDER BY a SETTINGS apply_row_policy_after_final = 0;

DROP ROW POLICY p_repl ON t_repl;
DROP TABLE t_repl;
