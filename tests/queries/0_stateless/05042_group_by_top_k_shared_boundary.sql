-- The shared top-K boundary lets every aggregation thread skip rows against the tightest
-- boundary any thread has published, instead of only its own. A set of `K` keys strictly
-- better than a row proves the row cannot reach the final result no matter which thread
-- holds them, so sharing must not change any result. The data below is descending and
-- clustered (each key occupies one contiguous run), the adversarial layout for per-thread
-- boundaries: threads that read early ranges hold only globally-poor keys until another
-- thread publishes a better boundary.
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
SET max_threads = 16;
-- Small blocks, so every thread processes several blocks and the boundary is exchanged between them.
SET max_block_size = 8192;

DROP TABLE IF EXISTS t_topk_shared_boundary;
CREATE TABLE t_topk_shared_boundary (k UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_topk_shared_boundary SELECT intDiv(400000 - number, 20) AS k, toString(k) AS s, number FROM numbers_mt(400000);

SELECT 'numeric key, shared boundary on vs off';
SELECT k, count(), sum(v) FROM t_topk_shared_boundary GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS group_by_top_k_optimization_shared_boundary = 1;
SELECT k, count(), sum(v) FROM t_topk_shared_boundary GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS group_by_top_k_optimization_shared_boundary = 0;
SELECT k, count(), sum(v) FROM t_topk_shared_boundary GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'DESC direction';
SELECT k, count() FROM t_topk_shared_boundary GROUP BY k ORDER BY k DESC LIMIT 5
SETTINGS group_by_top_k_optimization_shared_boundary = 1;
SELECT k, count() FROM t_topk_shared_boundary GROUP BY k ORDER BY k DESC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'string key (generic compare path)';
SELECT s, count() FROM t_topk_shared_boundary GROUP BY s ORDER BY s ASC LIMIT 5
SETTINGS group_by_top_k_optimization_shared_boundary = 1;
SELECT s, count() FROM t_topk_shared_boundary GROUP BY s ORDER BY s ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite key (tuple boundary)';
SELECT k, s, sum(v) FROM t_topk_shared_boundary GROUP BY k, s ORDER BY k ASC, s ASC LIMIT 5
SETTINGS group_by_top_k_optimization_shared_boundary = 1;
SELECT k, s, sum(v) FROM t_topk_shared_boundary GROUP BY k, s ORDER BY k ASC, s ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'prefix mode (skip-only, boundary shared on the prefix rank)';
SELECT k, s, count() FROM t_topk_shared_boundary GROUP BY k, s ORDER BY k ASC LIMIT 5
SETTINGS group_by_top_k_optimization_shared_boundary = 1;
SELECT k, s, count() FROM t_topk_shared_boundary GROUP BY k, s ORDER BY k ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_topk_shared_boundary;

-- Two non-nullable `LowCardinality(String)` keys select the serialized aggregation method, the one
-- method that serializes `LowCardinality` keys from their dictionary instead of materializing them
-- whenever the heap is inactive for the block (`Aggregator::executeOnBlock`). An active heap ranks
-- the materialized columns, so the inactivity decision must agree with what `executeImpl` does
-- after the shared-boundary exchange. This layout makes the two disagree unless the decision is
-- conservative: the first granules hold the five best keys and every other row one of five poor
-- keys, so a thread on a poor range fills its heap to `K` with keys it never evicts or skips, and
-- with the one-row observation window `shouldFreeze` turns true at its second block. That is also
-- when the boundary published by the thread on the first range reaches it and restarts its
-- window, keeping the heap running; the keys of that block must still be materialized.
SELECT 'LowCardinality keys (serialized method, freeze check racing the shared boundary)';
SET group_by_top_k_optimization_observation_rows = 1;

DROP TABLE IF EXISTS t_topk_shared_boundary_lc;
CREATE TABLE t_topk_shared_boundary_lc (lc LowCardinality(String), s LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_topk_shared_boundary_lc SELECT concat(if(number < 16384, 'a', 'z'), toString(number % 5)) AS lc, toString(number % 5) AS s, number FROM numbers(400000);

SELECT lc, s, count(), sum(v) FROM t_topk_shared_boundary_lc GROUP BY lc, s ORDER BY lc ASC, s ASC LIMIT 5
SETTINGS group_by_top_k_optimization_shared_boundary = 1;
SELECT lc, s, count(), sum(v) FROM t_topk_shared_boundary_lc GROUP BY lc, s ORDER BY lc ASC, s ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

-- The thread on the first range is now the one holding the poor keys.
SELECT lc, s, count(), sum(v) FROM t_topk_shared_boundary_lc GROUP BY lc, s ORDER BY lc DESC, s DESC LIMIT 5
SETTINGS group_by_top_k_optimization_shared_boundary = 1;
SELECT lc, s, count(), sum(v) FROM t_topk_shared_boundary_lc GROUP BY lc, s ORDER BY lc DESC, s DESC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

-- Prefix mode: the heap ranks `lc` alone while `s` is still a serialized key.
SELECT lc, s, count() FROM t_topk_shared_boundary_lc GROUP BY lc, s ORDER BY lc ASC LIMIT 5
SETTINGS group_by_top_k_optimization_shared_boundary = 1;
SELECT lc, s, count() FROM t_topk_shared_boundary_lc GROUP BY lc, s ORDER BY lc ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_topk_shared_boundary_lc;
