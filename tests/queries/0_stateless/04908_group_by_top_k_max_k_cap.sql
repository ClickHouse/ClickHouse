-- A LIMIT above the hard cap (TopKParams::max_k = 100000) never gets the
-- top-K optimization, even with the max-limit setting disabled.

SET serialize_query_plan = 0;
SET enable_group_by_top_k_optimization = 1;
SET query_plan_max_limit_for_top_k_optimization = 0;
-- The CI users profile sets `max_rows_to_group_by`, which gates the optimization off.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_top_k_cap;
CREATE TABLE t_top_k_cap (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_top_k_cap SELECT number, number FROM numbers(1000);

SELECT 'at the cap: optimized';
SELECT count() FROM
(
    EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_cap GROUP BY k ORDER BY k LIMIT 100000
)
WHERE explain LIKE '%Top-K%';

SELECT 'above the cap: not optimized';
SELECT count() FROM
(
    EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_cap GROUP BY k ORDER BY k LIMIT 100001
)
WHERE explain LIKE '%Top-K%';

DROP TABLE t_top_k_cap;
