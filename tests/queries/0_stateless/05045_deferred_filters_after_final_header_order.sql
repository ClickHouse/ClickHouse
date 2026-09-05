-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- A read step whose filters are deferred past FINAL must keep the output header it was planned with.
-- Under `make_distributed_plan` the consuming stage is built from that header before the producing
-- stage runs, and the exchange hands chunks over unchanged, so a header whose column order no longer
-- matches the emitted chunks pairs every column with another column's type.
--
-- Three things below are load bearing:
--   * `WITH CUBE` is what puts the aggregation behind a shuffle exchange, so the `WHERE` is evaluated
--     in the consuming stage. With a plain `GROUP BY` the read and the filter stay in one stage, the
--     filter takes its header from the pipe, and a wrong declared header cannot be observed at all.
--     The two `filter_above_exchange` assertions below pin that, so a plan change that fuses the
--     stages fails here instead of quietly leaving every arm green.
--   * `s LowCardinality(String)` is the mispaired column: paired with a numeric type it makes the
--     comparison fail outright instead of silently returning wrong values.
--   * The predicate must be over `v`, which is listed after `s` in the read set, so the two candidate
--     header orders differ. A predicate over the first read column hides the bug.
--
-- Every arm prints values, and each deferred arm is paired with the undeferred ground truths (filter
-- not deferred, and no distributed plan) which must agree with it.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET max_threads = 4;

DROP TABLE IF EXISTS t_hdr_05045;
CREATE TABLE t_hdr_05045 (k UInt64, v UInt64, s LowCardinality(String)) ENGINE = ReplacingMergeTree(v) ORDER BY k;
SYSTEM STOP MERGES t_hdr_05045;

-- One part per INSERT and no OPTIMIZE: FINAL has to deduplicate for the results below to hold, so an
-- arm that stopped merging would not silently look correct.
INSERT INTO t_hdr_05045 SELECT number, 1, 'a' FROM numbers(20);
INSERT INTO t_hdr_05045 SELECT number, 2, 'b' FROM numbers(20);

SELECT '-- fixture is multi-part';
SELECT count() > 1 AS multi_part FROM system.parts
WHERE database = currentDatabase() AND table = 't_hdr_05045' AND active
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- The deferred arms detect a wrong declared header only while the consuming `Filter` is separated
-- from `ReadFromMergeTree` by an exchange, which is what makes the header cross a stage boundary.
-- The second statement is the negative control: the same query with a plain `GROUP BY` keeps both in
-- one stage and prints 0. The explained query re-enables the two settings the surrounding statement
-- has to switch off, because a distributed plan cannot read from an `EXPLAIN`. The `countIf` guard is
-- not redundant: with no `Filter` row at all `minIf` returns 0, which satisfies the ordering.
SELECT 'filter above exchange',
       countIf(explain ILIKE '%Filter%') > 0
   AND minIf(n, explain ILIKE '%Filter%') < maxIf(n, explain ILIKE '%Exchange%')
   AND maxIf(n, explain ILIKE '%Exchange%') < maxIf(n, explain ILIKE '%ReadFromMergeTree%') AS filter_above_exchange
FROM (SELECT rowNumberInAllBlocks() AS n, explain FROM (
    EXPLAIN distributed = 1 SELECT s FROM t_hdr_05045 FINAL PREWHERE v <= 10 WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
    SETTINGS distributed_plan_execute_locally = 1, apply_prewhere_after_final = 1, enable_cascades_optimizer = 1, make_distributed_plan = 1))
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'filter above exchange, plain GROUP BY control',
       countIf(explain ILIKE '%Filter%') > 0
   AND minIf(n, explain ILIKE '%Filter%') < maxIf(n, explain ILIKE '%Exchange%')
   AND maxIf(n, explain ILIKE '%Exchange%') < maxIf(n, explain ILIKE '%ReadFromMergeTree%') AS filter_above_exchange
FROM (SELECT rowNumberInAllBlocks() AS n, explain FROM (
    EXPLAIN distributed = 1 SELECT s FROM t_hdr_05045 FINAL PREWHERE v <= 10 WHERE 10 > v GROUP BY s ORDER BY s
    SETTINGS distributed_plan_execute_locally = 1, apply_prewhere_after_final = 1, enable_cascades_optimizer = 1, make_distributed_plan = 1))
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- FINAL keeps v = 2 for every key, so s = 'b' survives and both predicates hold.
SELECT '-- deferred prewhere, filter above the exchange';
SELECT s FROM t_hdr_05045 FINAL PREWHERE v <= 10 WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
SETTINGS distributed_plan_execute_locally = 1, apply_prewhere_after_final = 1;
SELECT '-- ground truth: prewhere not deferred';
SELECT s FROM t_hdr_05045 FINAL PREWHERE v <= 10 WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
SETTINGS distributed_plan_execute_locally = 1, apply_prewhere_after_final = 0;
SELECT '-- ground truth: no distributed plan';
SELECT s FROM t_hdr_05045 FINAL PREWHERE v <= 10 WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
SETTINGS apply_prewhere_after_final = 1, enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- A bare column as the predicate: the deferred filter keeps its own predicate column in the stream
-- while the planned header holds it as a constant, so the two must still match by name and type.
SELECT '-- deferred prewhere on a bare column';
SELECT s FROM t_hdr_05045 FINAL PREWHERE v WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
SETTINGS distributed_plan_execute_locally = 1, apply_prewhere_after_final = 1;
SELECT '-- ground truth: bare column, no distributed plan';
SELECT s FROM t_hdr_05045 FINAL PREWHERE v WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
SETTINGS apply_prewhere_after_final = 1, enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- The deferred filter must still filter. `v = 1` is the losing version of every key, so after
-- deduplication nothing passes, while applying it before FINAL would admit all 20 keys.
SELECT '-- deferred filter still excludes rows';
SELECT count() FROM t_hdr_05045 FINAL PREWHERE v = 1 GROUP BY ALL WITH CUBE
SETTINGS distributed_plan_execute_locally = 1, apply_prewhere_after_final = 1;
SELECT '-- same predicate applied before final';
SELECT count() FROM t_hdr_05045 FINAL PREWHERE v = 1 GROUP BY ALL WITH CUBE
SETTINGS distributed_plan_execute_locally = 1, apply_prewhere_after_final = 0;

-- The emitted shape must stay exactly the table's columns: no column dropped, none leaked.
SELECT '-- select star under a deferred filter';
SELECT * FROM t_hdr_05045 FINAL PREWHERE v <= 10 WHERE 10 > v ORDER BY k LIMIT 3
SETTINGS distributed_plan_execute_locally = 1, apply_prewhere_after_final = 1;
SELECT '-- ground truth: select star, no distributed plan';
SELECT * FROM t_hdr_05045 FINAL PREWHERE v <= 10 WHERE 10 > v ORDER BY k LIMIT 3
SETTINGS apply_prewhere_after_final = 1, enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- A virtual column is resolved by a separate lookup when the read set is turned into a sample block,
-- and it is the shape the fuzzer found the defect with.
SELECT '-- deferred prewhere with a virtual column';
SELECT _table FROM t_hdr_05045 FINAL PREWHERE v <= 10 WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY _table
SETTINGS distributed_plan_execute_locally = 1, apply_prewhere_after_final = 1;
SELECT '-- ground truth: virtual column, no distributed plan';
SELECT _table FROM t_hdr_05045 FINAL PREWHERE v <= 10 WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY _table
SETTINGS apply_prewhere_after_final = 1, enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_hdr_05045;

-- A row policy over a non-sorting-key column defers through the same block, and needs no non-default
-- query setting: `apply_row_policy_after_final` is on by default.
DROP TABLE IF EXISTS t_rp_05045;
CREATE TABLE t_rp_05045 (k UInt64, v UInt64, s LowCardinality(String)) ENGINE = ReplacingMergeTree(v) ORDER BY k;
SYSTEM STOP MERGES t_rp_05045;
INSERT INTO t_rp_05045 SELECT number, 1, 'a' FROM numbers(20);
INSERT INTO t_rp_05045 SELECT number, 2, 'b' FROM numbers(20);

SELECT '-- fixture is multi-part';
SELECT count() > 1 AS multi_part FROM system.parts
WHERE database = currentDatabase() AND table = 't_rp_05045' AND active
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP ROW POLICY IF EXISTS policy_05045 ON t_rp_05045;
CREATE ROW POLICY policy_05045 ON t_rp_05045 USING v <= 10 TO ALL;

SELECT '-- deferred row policy at default settings';
SELECT s FROM t_rp_05045 FINAL WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
SETTINGS distributed_plan_execute_locally = 1;
SELECT '-- ground truth: policy not deferred';
SELECT s FROM t_rp_05045 FINAL WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
SETTINGS distributed_plan_execute_locally = 1, apply_row_policy_after_final = 0;
SELECT '-- ground truth: policy, no distributed plan';
SELECT s FROM t_rp_05045 FINAL WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- Both deferrals stacked.
SELECT '-- deferred row policy and deferred prewhere';
SELECT s FROM t_rp_05045 FINAL PREWHERE v <= 10 WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
SETTINGS distributed_plan_execute_locally = 1, apply_prewhere_after_final = 1;
SELECT '-- ground truth: stacked, no distributed plan';
SELECT s FROM t_rp_05045 FINAL PREWHERE v <= 10 WHERE 10 > v GROUP BY ALL WITH CUBE ORDER BY s
SETTINGS apply_prewhere_after_final = 1, enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- The policy must still filter: `v = 1` loses every deduplication group, so nothing survives.
DROP ROW POLICY policy_05045 ON t_rp_05045;
CREATE ROW POLICY policy_05045 ON t_rp_05045 USING v = 1 TO ALL;
SELECT '-- deferred row policy still excludes rows';
SELECT count() FROM t_rp_05045 FINAL GROUP BY ALL WITH CUBE
SETTINGS distributed_plan_execute_locally = 1;
SELECT '-- same policy applied before final';
SELECT count() FROM t_rp_05045 FINAL GROUP BY ALL WITH CUBE
SETTINGS distributed_plan_execute_locally = 1, apply_row_policy_after_final = 0;

DROP ROW POLICY policy_05045 ON t_rp_05045;
DROP TABLE t_rp_05045;
