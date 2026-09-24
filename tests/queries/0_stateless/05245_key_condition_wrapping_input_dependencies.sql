-- Tags: no-random-settings, no-random-merge-tree-settings
-- The part and granule counts require fixed index and query-plan settings.

SET explain_query_plan_default = 'legacy';
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SET use_query_condition_cache = 0;
SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;

-- Statistics use an input-only key expression. Each predicate must constrain its own column
-- without introducing dependencies on the other inputs.
CREATE TABLE wrapping_inputs (id UInt64, x UInt64, y UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic', add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES wrapping_inputs;
INSERT INTO wrapping_inputs SELECT number, 0, 1 FROM numbers(64);
INSERT INTO wrapping_inputs SELECT number + 64, 1, 0 FROM numbers(64);

SELECT 'Input-only statistics key';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(id) FROM wrapping_inputs WHERE x = 1 AND y = 0
SETTINGS use_statistics_for_part_pruning = 0)
WHERE explain LIKE '%Parts:%';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(id) FROM wrapping_inputs WHERE x = 1 AND y = 0
SETTINGS use_statistics_for_part_pruning = 1)
WHERE explain LIKE '%Parts:%';
SELECT count(), sum(id) FROM wrapping_inputs WHERE x = 1 AND y = 0;
DROP TABLE wrapping_inputs;

-- The aliased derived key improves pruning through `x`. Unrelated input keys remain in the
-- expression, and the derived candidate must remain available to both comparisons and sets.
CREATE TABLE wrapping_derived (u UInt16, x UInt16, y UInt16) ENGINE = MergeTree
ORDER BY (u, toUInt8(x) AS bucket, x, y)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO wrapping_derived SELECT 0, number, 7 FROM numbers(512);

SELECT 'Aliased derived key, equality';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(x) FROM wrapping_derived WHERE u = 0 AND x = 257 AND y = 7
SETTINGS analyze_index_with_multiple_key_columns_per_condition = 0)
WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(x) FROM wrapping_derived WHERE u = 0 AND x = 257 AND y = 7)
WHERE explain LIKE '%Granules:%';
SELECT count(), sum(x) FROM wrapping_derived WHERE u = 0 AND x = 257 AND y = 7;

SELECT 'Aliased derived key, set';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(x) FROM wrapping_derived WHERE u = 0 AND x IN (257, 259) AND y = 7
SETTINGS analyze_index_with_multiple_key_columns_per_condition = 0)
WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(x) FROM wrapping_derived WHERE u = 0 AND x IN (257, 259) AND y = 7)
WHERE explain LIKE '%Granules:%';
SELECT count(), sum(x) FROM wrapping_derived WHERE u = 0 AND x IN (257, 259) AND y = 7;
DROP TABLE wrapping_derived;
