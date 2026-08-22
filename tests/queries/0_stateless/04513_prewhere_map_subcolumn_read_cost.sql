-- Regression test: the PREWHERE optimizer must account for Map-key subcolumn read cost.
-- When `optimize_functions_to_subcolumns` rewrites `h['k']` to the `h.key_k` subcolumn,
-- the optimizer must see its actual on-disk size (the whole Map) via getSubcolumnSize
-- and place the cheap `modality` filter before the expensive Map-key predicate.
--
-- We disable statistics so that only columns_size drives the ordering,
-- and use equality conditions on both sides so that both are "good" conditions
-- (isConditionGood only returns true for equals).

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;
SET use_statistics = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_prewhere_map_cost;
CREATE TABLE t_prewhere_map_cost (id UInt64, modality LowCardinality(String), h Map(String, String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_prewhere_map_cost
SELECT number, if(number < 1000, 'active', ''), map('k', repeat('v', 300), 'k2', repeat('w', 300))
FROM numbers(200000);
OPTIMIZE TABLE t_prewhere_map_cost FINAL;

SELECT '-- cheap modality filter is placed before the Map-key predicate';
SELECT position(explain, 'modality') > 0 AND position(explain, 'modality') < position(explain, 'h.key_k') AS cheap_first
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_map_cost WHERE modality = '' AND h['k'] = 'nope'
) WHERE explain LIKE '%Prewhere filter column%';

SELECT '-- correctness: result is the same regardless of ordering';
SELECT count() FROM t_prewhere_map_cost WHERE modality = '' AND h['k'] = 'nope';
SELECT count() FROM t_prewhere_map_cost WHERE modality = '' AND h['k'] = 'nope'
SETTINGS allow_reorder_prewhere_conditions = 0;

-- Same check through the legacy InterpreterSelectQuery PREWHERE path
-- (disable both the analyzer and the plan-based PREWHERE optimizer).
SET enable_analyzer = 0;
SET query_plan_optimize_prewhere = 0;

SELECT '-- legacy InterpreterSelectQuery path: cheap filter first';
SELECT position(explain, 'modality') > 0 AND position(explain, 'modality') < position(explain, 'arrayElement') AS cheap_first
FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_prewhere_map_cost WHERE modality = '' AND h['k'] = 'nope'
) WHERE explain LIKE '%Prewhere filter column%';

DROP TABLE t_prewhere_map_cost;
