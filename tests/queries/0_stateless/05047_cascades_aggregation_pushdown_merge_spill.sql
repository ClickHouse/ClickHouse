-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- End-to-end external-aggregation (spill) coverage for the merge-only `Aggregating` above the
-- pushed variant-A join. The full-`Params` copy in the rule and the settings round trip of the
-- serialized fragment must deliver `max_bytes_before_external_group_by` (and the two-level
-- thresholds - `mergeOnBlock` spills only after the two-level conversion) to the worker that
-- executes the merge; result equality alone proves nothing about the deserialized limit being
-- honored, so server-side spill evidence is asserted below.
--
-- ATTRIBUTION: the spill events are query-wide and the pushed partial aggregation carries the
-- same spill settings (full-`Params` copy), so the scenario is built to be structurally
-- attributable: the pushed side groups by the lone join-condition column `j` with only 2 distinct
-- values - the partial's hash table can never reach `group_by_two_level_threshold = 256` rows
-- (and the bytes threshold is disabled), so it can never convert to two-level and never spill.
-- The rebuilt join then fans those 2 state rows out to 100000 distinct `g` values, so only the
-- top merge crosses the two-level threshold and the external-aggregation limit.

DROP TABLE IF EXISTS t_spill_facts;
DROP TABLE IF EXISTS t_spill_dims;

CREATE TABLE t_spill_facts (j UInt32, v Int64) ENGINE = MergeTree ORDER BY j
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_spill_dims (j UInt32, g UInt32) ENGINE = MergeTree ORDER BY j
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_spill_facts;
SYSTEM STOP MERGES t_spill_dims;

INSERT INTO t_spill_facts SELECT number % 2, number FROM numbers(10000);
INSERT INTO t_spill_dims SELECT number % 2, number FROM numbers(100000);

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_spill_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"j": 2}}, "t_spill_dims": {"cardinality": 100000, "avg_row_bytes": 8, "distinct_keys": {"j": 2, "g": 100000}}}';
-- Spill knobs, pinned against harness randomization: a low external-aggregation limit alone
-- cannot spill - `mergeOnBlock` additionally requires the two-level conversion, so the rows
-- threshold is pinned low too (and its bytes twin disabled, keeping the 2-group partial
-- single-level whatever its byte size).
SET group_by_two_level_threshold = 256;
SET group_by_two_level_threshold_bytes = 0;
SET max_bytes_before_external_group_by = 10000;
SET max_bytes_ratio_before_external_group_by = 0;

-- Confirm variant A fired, pinned as the full legacy EXPLAIN: the sandwich - the merge-only
-- `Aggregating` above the `JoinLogical`, the partial `Aggregating` below it. The runtime-filter
-- and prewhere settings decide the `BuildRuntimeFilter`/`Filter` lines of the INNER shape and are
-- randomized by the harness, so they are pinned in the EXPLAIN's SETTINGS clause only - a
-- session-level `SET` would leak into the executed spill probe below.
SELECT '-- canary: variant A fires for the spill query';
EXPLAIN SELECT t2.g AS g, count() AS c, sum(t1.v) AS s FROM t_spill_facts AS t1 INNER JOIN t_spill_dims AS t2 ON t1.j = t2.j GROUP BY t2.g
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, explain_query_plan_default = 'legacy',
    enable_join_runtime_filters = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SELECT t2.g AS g, count() AS c, sum(t1.v) AS s FROM t_spill_facts AS t1 INNER JOIN t_spill_dims AS t2 ON t1.j = t2.j GROUP BY t2.g
FORMAT Null
SETTINGS log_comment = '05047_cascades_spill_probe';

-- Spill evidence from `system.text_log` rather than `ProfileEvents` in `system.query_log`:
-- under `distributed_plan_execute_locally` the fragment pipelines run on executor-pool threads
-- whose profile counters do not reach any `query_log` entry (verified: the initiator and every
-- `stage_*`/`main` fragment entry report `ExternalAggregationWritePart = 0` while the server log
-- shows the writes), so the `Aggregator` log line is the reliable server-side proof. The
-- `query_log` subquery collects the whole fragment family: `log_comment` propagates to the
-- fragment queries. The evidence query must be planned classically: a distributed plan pins
-- concrete log-table part names at planning time, and background merges of the log tables
-- invalidate them between planning and fragment execution.
SYSTEM FLUSH LOGS query_log, text_log;
SELECT '-- merge spilled to disk';
SELECT count() > 0 AS merge_spilled
FROM system.text_log
WHERE query_id IN (SELECT query_id FROM system.query_log WHERE log_comment = '05047_cascades_spill_probe' AND current_database = currentDatabase())
  AND logger_name = 'Aggregator' AND message LIKE 'Writing part of aggregation data%'
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- Result check: an order-insensitive digest of the full 100000-group result, compared against
-- hand-computed constants of the deterministic dataset:
--   groups = 100000: the dims carry 100000 distinct `g`;
--   total_rows = 500000000: each of the 100000 `g` groups receives `c` = 5000 fact rows of its
--     join key (facts hold 5000 rows per `j`);
--   total_sum = 2499750000000: every fact row matches the 50000 dim rows of its `j`, so the sum
--     is 50000 x (sum of `v` over the facts) = 50000 x 49995000.
-- Pushed-vs-classic equality is covered by 04927 on sane sizes; the classic plan here would
-- materialize the 500M-row join fan-out this optimization exists to avoid (it exceeded the query
-- memory limit in CI with the session spill knobs).
SELECT '-- digest: pushed (with spill) vs hand-computed constants';
SELECT count() AS groups, sum(c) AS total_rows, sum(s) AS total_sum FROM
(
    SELECT t2.g AS g, count() AS c, sum(t1.v) AS s FROM t_spill_facts AS t1 INNER JOIN t_spill_dims AS t2 ON t1.j = t2.j GROUP BY t2.g
);

DROP TABLE t_spill_facts;
DROP TABLE t_spill_dims;
