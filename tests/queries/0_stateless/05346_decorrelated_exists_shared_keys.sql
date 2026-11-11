-- A correlated `EXISTS` with a condition the join keys cannot express becomes a semi join whose
-- probe side is the subquery joined with the distinct correlation keys of the preserved side, so
-- every probe key is a preserved key. Without this the two sides
-- counted as independent samples of the key domain: the outer rows below 1000 keep 1000 of the
-- 10000 keys, the probe side has the same 1000, and the semi join estimated a tenth of the rows.
SET allow_experimental_statistics = 1;
-- `EXISTS` is a semi join only through this conversion; the runner randomizes it.
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
-- The columns a step does not need are dropped below it; the runner randomizes the removal.
SET query_plan_remove_unused_columns = 1;
-- The plan lines below show the two filters merged into one expression; the runner randomizes the merge.
SET query_plan_merge_filters = 1;
-- The correlation equality is a key of the domain join only when the filter is merged into the join
-- condition; the runner randomizes the merge.
SET query_plan_merge_filter_into_join_condition = 1;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET allow_correlated_subqueries = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;

DROP TABLE IF EXISTS t_dx_outer;
DROP TABLE IF EXISTS t_dx_inner;

-- Sparse serialization changes the bytes a read costs; the runner randomizes it.
CREATE TABLE t_dx_outer (k UInt64 STATISTICS(uniq, basic), v UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_dx_inner (k UInt64 STATISTICS(uniq, basic), w UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_dx_outer SELECT number, number FROM numbers(10000);
INSERT INTO t_dx_inner SELECT number, number + 1 FROM numbers(10000);

SELECT '-- EXISTS: every outer row below 1000 has a match, 1000 rows';
EXPLAIN estimates = 1 SELECT count() FROM t_dx_outer AS o WHERE o.v < 1000 AND EXISTS (SELECT 1 FROM t_dx_inner AS i WHERE i.k = o.k AND i.w != o.v);
SELECT count() FROM t_dx_outer AS o WHERE o.v < 1000 AND EXISTS (SELECT 1 FROM t_dx_inner AS i WHERE i.k = o.k AND i.w != o.v);

-- The anti join keeps the domain-based fraction, which errs towards keeping rows: the complement
-- of a share near one would turn a small error in the key counts into an estimate of one row.
SELECT '-- NOT EXISTS: no outer row is without a match; the estimate stays conservative';
EXPLAIN estimates = 1 SELECT count() FROM t_dx_outer AS o WHERE o.v < 1000 AND NOT EXISTS (SELECT 1 FROM t_dx_inner AS i WHERE i.k = o.k AND i.w != o.v);
SELECT count() FROM t_dx_outer AS o WHERE o.v < 1000 AND NOT EXISTS (SELECT 1 FROM t_dx_inner AS i WHERE i.k = o.k AND i.w != o.v);

DROP TABLE t_dx_outer;
DROP TABLE t_dx_inner;
