SET use_statistics = 0;
SET use_query_condition_cache = 0;
CREATE TABLE skip_order_pruned (p UInt64, k UInt64, x UInt64, y UInt64,
    INDEX ix x TYPE minmax GRANULARITY 1,
    INDEX iy y TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS index_granularity = 16;
INSERT INTO skip_order_pruned SELECT number % 16, number, number % 97, number % 37 FROM numbers(8192);
SELECT count(), sum(k) FROM skip_order_pruned WHERE p = 15 AND x = 11 SETTINGS use_skip_indexes = 0, per_part_index_stats = 1;
SELECT count(), sum(k) FROM skip_order_pruned WHERE p = 15 AND x = 11 SETTINGS use_skip_indexes = 1, per_part_index_stats = 1;
SELECT count(), sum(k) FROM skip_order_pruned WHERE p IN (7, 15) AND (x = 11 OR y = 7) SETTINGS use_skip_indexes = 0, per_part_index_stats = 1;
SELECT count(), sum(k) FROM skip_order_pruned WHERE p IN (7, 15) AND (x = 11 OR y = 7) SETTINGS use_skip_indexes = 1, per_part_index_stats = 1;
SELECT count(), sum(k) FROM skip_order_pruned WHERE p IN (7, 15) AND x < 50 AND y < 20 SETTINGS use_skip_indexes = 0, per_part_index_stats = 1;
SELECT count(), sum(k) FROM skip_order_pruned WHERE p IN (7, 15) AND x < 50 AND y < 20 SETTINGS use_skip_indexes = 1, per_part_index_stats = 1;
SELECT count(), sum(k) FROM skip_order_pruned WHERE p = 99 AND x = 11 SETTINGS use_skip_indexes = 0, per_part_index_stats = 1;
SELECT count(), sum(k) FROM skip_order_pruned WHERE p = 99 AND x = 11 SETTINGS use_skip_indexes = 1, per_part_index_stats = 1;
DROP TABLE skip_order_pruned;
