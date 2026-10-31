-- An `ORDER BY ... LIMIT` read filtered on an ALIAS column gets no threshold filter when a stored column has the
-- name of the PREWHERE condition the filter would form with it. The rows are the same either way.

DROP TABLE IF EXISTS t_topk_collide_and;
DROP TABLE IF EXISTS t_topk_alias;
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0; -- the EXPLAIN checks assert on the local read step
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET query_plan_remove_unused_columns = 1; -- keeps the colliding column out of the read output

DROP TABLE IF EXISTS t_topk_collide_and;
DROP TABLE IF EXISTS t_topk_alias;
CREATE TABLE t_topk_collide_and (k UInt32, `and(__topKFilter(k), __table1.c)` UInt8, c UInt8 ALIAS `and(__topKFilter(k), __table1.c)` = 0)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
CREATE TABLE t_topk_alias (k UInt32, other UInt8, c UInt8 ALIAS other = 0)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_collide_and SELECT number, number % 3 FROM numbers(50000);
INSERT INTO t_topk_alias SELECT number, number % 3 FROM numbers(50000);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT k FROM t_topk_collide_and WHERE c ORDER BY k LIMIT 5)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';
SELECT groupArray(k) FROM (SELECT k FROM t_topk_collide_and WHERE c ORDER BY k LIMIT 5)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_topk_collide_and WHERE c ORDER BY k LIMIT 5)
SETTINGS use_top_k_dynamic_filtering = 1;

-- The same read of a column with another name does get the filter.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT k FROM t_topk_alias WHERE c ORDER BY k LIMIT 5)
WHERE explain ILIKE '%Prewhere filter column: and(\_\_topKFilter(k), \_\_table1.c)%';

DROP TABLE t_topk_collide_and;
DROP TABLE t_topk_alias;
