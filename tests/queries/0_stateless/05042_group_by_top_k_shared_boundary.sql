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

DROP TABLE IF EXISTS t_topk_shared_boundary;
CREATE TABLE t_topk_shared_boundary (k UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_topk_shared_boundary SELECT intDiv(2000000 - number, 20) AS k, toString(k) AS s, number FROM numbers_mt(2000000);

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
