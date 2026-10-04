-- A probe that emits no right column, such as `SELECT count()`, cannot count matches at all, so its
-- zero is structural rather than measured and must not become the cached match count for the join.
-- Otherwise the next full output run over the same join reads that zero and loses the row store.

DROP TABLE IF EXISTS right_wide;
DROP TABLE IF EXISTS right_wide_ph;

-- Pin planner settings so the row store decision comes from the recorded match count alone
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET use_statistics = 0;
SET collect_hash_table_stats_during_joins = 1;
SET param__internal_join_table_stat_hints = '{}';

CREATE TABLE right_wide (k UInt64, v1 Int64, v2 UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO right_wide SELECT number, number, number FROM numbers(20000);

-- Separate table for `parallel_hash`: `join_algorithm` is not part of the match stats cache key.
CREATE TABLE right_wide_ph (k UInt64, v1 Int64, v2 UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO right_wide_ph SELECT number, number, number FROM numbers(20000);

SELECT r.v1, r.v2 FROM (SELECT number % 20000 AS k FROM numbers(200000)) p JOIN right_wide r ON p.k = r.k FORMAT Null
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, min_rows_ratio_for_hash_join_row_store = 0, log_comment = 'rs_hash_1_warm';

SELECT count() FROM (SELECT number % 20000 AS k FROM numbers(200000)) p JOIN right_wide r ON p.k = r.k FORMAT Null
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, min_rows_ratio_for_hash_join_row_store = 2, log_comment = 'rs_hash_2_no_emit_probe';

SELECT r.v1, r.v2 FROM (SELECT number % 20000 AS k FROM numbers(200000)) p JOIN right_wide r ON p.k = r.k FORMAT Null
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, min_rows_ratio_for_hash_join_row_store = 2, log_comment = 'rs_hash_3_after_no_emit_probe';

SELECT r.v1, r.v2 FROM (SELECT number % 20000 AS k FROM numbers(200000)) p JOIN right_wide_ph r ON p.k = r.k FORMAT Null
SETTINGS join_algorithm = 'parallel_hash', query_plan_join_swap_table = 0, min_rows_ratio_for_hash_join_row_store = 0, log_comment = 'rs_par_1_warm';

SELECT count() FROM (SELECT number % 20000 AS k FROM numbers(200000)) p JOIN right_wide_ph r ON p.k = r.k FORMAT Null
SETTINGS join_algorithm = 'parallel_hash', query_plan_join_swap_table = 0, min_rows_ratio_for_hash_join_row_store = 2, log_comment = 'rs_par_2_no_emit_probe';

SELECT r.v1, r.v2 FROM (SELECT number % 20000 AS k FROM numbers(200000)) p JOIN right_wide_ph r ON p.k = r.k FORMAT Null
SETTINGS join_algorithm = 'parallel_hash', query_plan_join_swap_table = 0, min_rows_ratio_for_hash_join_row_store = 2, log_comment = 'rs_par_3_after_no_emit_probe';

SYSTEM FLUSH LOGS query_log;

-- The `_2_no_emit_probe` rows read 0 and show the counter can report an unbuilt row store; the
-- `_1_warm` and `_3_after_no_emit_probe` rows must read 1.
SELECT log_comment, ProfileEvents['JoinBuildRowStoreMicroseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
  AND log_comment IN ('rs_hash_1_warm', 'rs_hash_2_no_emit_probe', 'rs_hash_3_after_no_emit_probe',
                      'rs_par_1_warm', 'rs_par_2_no_emit_probe', 'rs_par_3_after_no_emit_probe')
ORDER BY log_comment;

DROP TABLE right_wide_ph;
DROP TABLE right_wide;
