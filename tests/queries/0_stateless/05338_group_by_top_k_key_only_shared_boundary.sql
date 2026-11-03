-- `GROUP BY ... ORDER BY ... LIMIT` without aggregate functions under the top-K optimization with the shared skip boundary, on key-clustered data.
DROP TABLE IF EXISTS t_topk_key_only;
SET max_rows_to_group_by = 0;
SET enable_group_by_top_k_optimization = 1;
SET group_by_top_k_optimization_shared_boundary = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET max_threads = 16;
SET max_block_size = 8192;

DROP TABLE IF EXISTS t_topk_key_only;
CREATE TABLE t_topk_key_only (k UInt64, s String, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_topk_key_only SELECT intDiv(400000 - number, 20) AS k, toString(k), if(k % 7 = 3, NULL, k) FROM numbers(400000);

SELECT 'numeric ASC';
SELECT k FROM t_topk_key_only GROUP BY k ORDER BY k ASC LIMIT 5;
SELECT 'numeric DESC';
SELECT k FROM t_topk_key_only GROUP BY k ORDER BY k DESC LIMIT 5;
SELECT 'string';
SELECT s FROM t_topk_key_only GROUP BY s ORDER BY s ASC LIMIT 5;
SELECT 'composite';
SELECT k, s FROM t_topk_key_only GROUP BY k, s ORDER BY k ASC, s ASC LIMIT 5;
SELECT 'nullable';
SELECT n FROM t_topk_key_only GROUP BY n ORDER BY n ASC NULLS FIRST LIMIT 5;

DROP TABLE t_topk_key_only;
