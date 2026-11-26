-- Regression test: TopK threshold filtering and minmax-based granule skipping
-- (`use_top_k_dynamic_filtering`, `use_skip_indexes_for_top_k`) must be disabled
-- for `FINAL` queries. Both rely on dropping rows that are beyond the running
-- top-K threshold; for an engine like `ReplacingMergeTree`, those dropped rows
-- may be duplicates that the deduplication pass needs to compare against.

DROP TABLE IF EXISTS t_top_k_final;

CREATE TABLE t_top_k_final
(
    key UInt64,
    version UInt64,
    sort_col UInt64,
    payload String,
    INDEX sort_col_idx sort_col TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version)
ORDER BY key
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_top_k_final;

-- Two overlapping parts. The newer part (version=2) updates payload to 'new'
-- for the first half of keys. With FINAL, every row should report 'new' or 'old'
-- according to which version wins, never a stale row that should have been replaced.
INSERT INTO t_top_k_final SELECT number, 1, number, 'old' FROM numbers(10000);
INSERT INTO t_top_k_final SELECT number, 2, number, 'new' FROM numbers(5000);

-- TopK dynamic filtering ON: must produce correct results because the optimizer
-- skips the optimization for FINAL.
SELECT '-- top-k dynamic filtering with FINAL';
SELECT key, sort_col, payload FROM t_top_k_final FINAL ORDER BY sort_col LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 0,
         query_plan_max_limit_for_top_k_optimization = 100, enable_parallel_replicas = 0;

-- Skip-indexes for top-k ON: must produce correct results.
SELECT '-- skip-indexes for top-k with FINAL';
SELECT key, sort_col, payload FROM t_top_k_final FINAL ORDER BY sort_col LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 1,
         query_plan_max_limit_for_top_k_optimization = 100, enable_parallel_replicas = 0;

-- Both ON: must produce correct results.
SELECT '-- both top-k optimizations with FINAL';
SELECT key, sort_col, payload FROM t_top_k_final FINAL ORDER BY sort_col LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1,
         query_plan_max_limit_for_top_k_optimization = 100, enable_parallel_replicas = 0;

-- The plan must not contain the dynamic top-K prewhere filter for FINAL queries.
SELECT '-- explain: no __topKFilter for FINAL';
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT key, sort_col FROM t_top_k_final FINAL ORDER BY sort_col LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1,
             query_plan_max_limit_for_top_k_optimization = 100, enable_parallel_replicas = 0
) WHERE explain LIKE '%__topKFilter%';

-- Without FINAL, the optimization is still applied (sanity check).
SELECT '-- explain: __topKFilter present without FINAL';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT key, sort_col FROM t_top_k_final ORDER BY sort_col LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1,
             query_plan_max_limit_for_top_k_optimization = 100, enable_parallel_replicas = 0
) WHERE explain LIKE '%__topKFilter%';

DROP TABLE t_top_k_final;
