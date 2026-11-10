-- Regression test: when statistics are enabled, the PREWHERE optimizer must
-- combine selectivity (estimated_row_count) with I/O cost (columns_size)
-- using the classic cost / (1 - selectivity) rule, not sort by selectivity alone.
--
-- Without this fix, the auto-collected Uniq statistic on `modality` (2 distinct
-- values → 50% selectivity) makes the optimizer place the expensive Map-key
-- predicate first because its default 1% selectivity looks "more selective",
-- ignoring that the Map column is ~500x more expensive to read.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;
SET use_statistics = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_prewhere_stats_cost;
CREATE TABLE t_prewhere_stats_cost (id UInt64, modality LowCardinality(String), h Map(String, String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

-- Split `modality` 50/50: the reject-count ratio is then ~2x while the Map column dwarfs the
-- scalar, so the cost gap dominates and cheap-filter-first stays stable under randomized serialization.
INSERT INTO t_prewhere_stats_cost
SELECT number, if(number % 2 = 0, 'active', ''), map('k', repeat('v', 300), 'k2', repeat('w', 300))
FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_stats_cost FINAL;

SELECT '-- with statistics: cheap filter is placed before the expensive Map predicate';
SELECT position(explain, 'modality') > 0 AND position(explain, 'modality') < position(explain, 'arrayElement') AS cheap_first
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_stats_cost WHERE modality = '' AND h['k'] = 'nope'
) WHERE explain LIKE '%Prewhere filter column%';

SELECT '-- correctness: result is the same regardless of ordering';
SELECT count() FROM t_prewhere_stats_cost WHERE modality = '' AND h['k'] = 'nope';
SELECT count() FROM t_prewhere_stats_cost WHERE modality = '' AND h['k'] = 'nope'
SETTINGS allow_reorder_prewhere_conditions = 0;

DROP TABLE t_prewhere_stats_cost;
