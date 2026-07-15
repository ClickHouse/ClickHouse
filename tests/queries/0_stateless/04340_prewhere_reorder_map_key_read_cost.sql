-- Regression for #110462: the PREWHERE reorderer must not hoist a Map-key predicate ahead of a
-- cheaper, more selective filter. Evaluating `h['k'] = const` reads the whole `Map` column, so it
-- must be charged its read cost and stay after the cheap `modality != ''` filter. Runs against a
-- real server (persisted per-column sizes); a single all-in-one clickhouse-local masks the bug.
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1; -- CI may inject 0, which would skip reordering
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_prewhere_map_cost;
CREATE TABLE t_prewhere_map_cost (id UInt64, modality LowCardinality(String), h Map(String, String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

-- `modality` is a tiny, selective column; `h` is a fat Map (two 300-byte values) that is expensive
-- to read in full. `OPTIMIZE FINAL` makes a single wide part so per-column sizes exist.
INSERT INTO t_prewhere_map_cost
SELECT number, if(number < 1000, 'active', ''), map('k', repeat('v', 300), 'k2', repeat('w', 300))
FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_map_cost FINAL;

SELECT '-- cheap modality filter is placed before the Map-key predicate';
SELECT position(explain, 'modality') > 0 AND position(explain, 'modality') < position(explain, 'h.key_k') AS cheap_first
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_map_cost WHERE modality != '' AND h['k'] = 'nope'
) WHERE explain LIKE '%Prewhere filter column%';

SELECT '-- correctness: result is the same regardless of ordering';
SELECT count() FROM t_prewhere_map_cost WHERE modality != '' AND h['k'] = 'nope';
SELECT count() FROM t_prewhere_map_cost WHERE modality != '' AND h['k'] = 'nope'
SETTINGS allow_reorder_prewhere_conditions = 0;

DROP TABLE t_prewhere_map_cost;
