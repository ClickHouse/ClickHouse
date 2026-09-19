SET use_query_cache = 0;
SET use_query_condition_cache = 0;
CREATE TABLE prewhere_sizes_pruned
(
    p UInt64, k UInt64, j JSON(a UInt64, b String), payload String
)
ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS min_bytes_for_full_part_storage = '5G', min_bytes_for_wide_part = 0;
INSERT INTO prewhere_sizes_pruned
SELECT number % 32, number,
    concat('{"a":', toString(number % 11), ',"b":"', toString(number % 7), '"}'),
    repeat('x', if(number % 32 = 31, 10, 100))
FROM numbers(8192);
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p = 31 AND j.a > 5 AND j.b = '3' SETTINGS use_statistics = 0, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1, use_partition_pruning = 1;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p IN (7, 31) AND j.a > 5 SETTINGS use_statistics = 0, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1, use_partition_pruning = 1;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p = 99 AND j.a > 5 SETTINGS use_statistics = 0, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1, use_partition_pruning = 1;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p = 31 AND j.a > 5 AND j.b = '3' SETTINGS use_statistics = 1, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1, use_partition_pruning = 1;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p IN (7, 31) AND j.a > 5 SETTINGS use_statistics = 1, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1, use_partition_pruning = 1;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p = 99 AND j.a > 5 SETTINGS use_statistics = 1, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1, use_partition_pruning = 1;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p = 31 AND j.a > 5 AND j.b = '3' SETTINGS use_statistics = 1, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0, use_partition_pruning = 1;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p IN (7, 31) AND j.a > 5 SETTINGS use_statistics = 1, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0, use_partition_pruning = 1;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p = 99 AND j.a > 5 SETTINGS use_statistics = 1, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0, use_partition_pruning = 1;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p = 31 AND j.a > 5 AND j.b = '3' SETTINGS use_statistics = 1, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1, use_partition_pruning = 0;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p IN (7, 31) AND j.a > 5 SETTINGS use_statistics = 1, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1, use_partition_pruning = 0;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE p = 99 AND j.a > 5 SETTINGS use_statistics = 1, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1, use_partition_pruning = 0;
ALTER TABLE prewhere_sizes_pruned ADD COLUMN added UInt64 DEFAULT k + 1;
SELECT count(), sum(added) FROM prewhere_sizes_pruned WHERE p = 31 AND added > 4000;
SELECT count(), sum(k) FROM prewhere_sizes_pruned WHERE j.a > 5;
DROP TABLE prewhere_sizes_pruned;

-- Planner-visible check of the JSON-subcolumn path: the moved conditions must follow the sizes of the pruned parts.
-- Partition 3 keeps `j.b` tiny and `payload` large, every other partition stores a huge `j.b` and a tiny `payload`.
-- Table-wide `j.b` dominates the queried bytes and stays in WHERE; measured on the single part left after pruning
-- by `p = 3` it is the cheapest column and moves to PREWHERE together with `c`.
SET enable_analyzer = 1, explain_query_plan_default = 'legacy';
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 0;
SET enable_multiple_prewhere_read_steps = 1, allow_reorder_prewhere_conditions = 0, query_plan_merge_filters = 1;
SET optimize_functions_to_subcolumns = 0, query_plan_optimize_primary_key = 1, enable_parallel_replicas = 0;
SET use_statistics = 0, allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1, use_partition_pruning = 1;

CREATE TABLE prewhere_sizes_pruned_plan
(
    p UInt64, k UInt64, j JSON(a UInt64, b String) CODEC(NONE), c String CODEC(NONE), payload String CODEC(NONE)
)
ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO prewhere_sizes_pruned_plan
SELECT number % 4, number,
    concat('{"a":', toString(number % 11), ',"b":"', repeat(toString(number % 7), if(number % 4 = 3, 1, 4096)), '"}'),
    repeat('c', 16),
    repeat('x', if(number % 4 = 3, 1024, 8))
FROM numbers(4000);

SELECT 'pruned to partition 3';
SELECT replaceRegexpAll(explain, '__table1\.|_String', '')
FROM (EXPLAIN actions = 1 SELECT sum(length(payload)) FROM prewhere_sizes_pruned_plan WHERE p = 3 AND notEmpty(j.b) AND notEmpty(c))
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Filter column%';

-- Nothing pruned: the estimate is the table-wide one and `j.b` is too heavy to move.
SELECT 'all partitions';
SELECT replaceRegexpAll(explain, '__table1\.|_String', '')
FROM (EXPLAIN actions = 1 SELECT sum(length(payload)) FROM prewhere_sizes_pruned_plan WHERE p < 4 AND notEmpty(j.b) AND notEmpty(c))
WHERE explain LIKE '%Prewhere filter column%' OR explain LIKE '%Filter column%';

SELECT sum(length(payload)) FROM prewhere_sizes_pruned_plan WHERE p = 3 AND notEmpty(j.b) AND notEmpty(c);

DROP TABLE prewhere_sizes_pruned_plan;
