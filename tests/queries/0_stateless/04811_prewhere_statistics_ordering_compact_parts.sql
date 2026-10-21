-- Regression test: on compact parts the optimizer cannot read per-column compressed
-- sizes, so the PREWHERE `cost_with_selectivity` score has no I/O-cost term (columns_size
-- is 0). It must still order conditions by selectivity (estimated_row_count) instead of
-- collapsing every condition to the same cost and keeping the original WHERE order.
-- See https://github.com/ClickHouse/ClickHouse/pull/110695

SET enable_analyzer = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1; -- CI may inject False, preventing statistics-based reordering
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_prewhere_stats_compact;

-- No `min_bytes_for_wide_part` override, so the part is compact and per-column sizes are 0.
CREATE TABLE t_prewhere_stats_compact
(
    id UInt64,
    x Int64 STATISTICS(tdigest),
    y Int64 STATISTICS(tdigest)
) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';

INSERT INTO t_prewhere_stats_compact SELECT number, number, number FROM numbers(100000);

-- `x < 10` matches 10 rows (very selective); `y < 100000` matches all rows (not selective).
-- The non-selective condition is written first in WHERE, but the optimizer must place the
-- selective `x` condition first in PREWHERE even though columns_size is 0 on the compact part.
SELECT '-- compact part: selective condition ordered first despite zero column sizes';
SELECT position(prewhere_line, 'less(x') > 0
   AND position(prewhere_line, 'less(x') < position(prewhere_line, 'less(y') AS x_first
FROM (
    SELECT extractAll(replaceRegexpAll(explain, '__table1\.', ''), 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions = 1 SELECT count() FROM t_prewhere_stats_compact WHERE y < 100000 AND x < 10
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT '-- correctness: result is the same regardless of ordering';
SELECT count() FROM t_prewhere_stats_compact WHERE y < 100000 AND x < 10;
SELECT count() FROM t_prewhere_stats_compact WHERE y < 100000 AND x < 10
SETTINGS allow_reorder_prewhere_conditions = 0;

DROP TABLE t_prewhere_stats_compact;
