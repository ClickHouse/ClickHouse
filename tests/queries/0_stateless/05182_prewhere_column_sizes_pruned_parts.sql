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
