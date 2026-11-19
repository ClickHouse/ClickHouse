SET explain_query_plan_default = 'legacy';
DROP TABLE IF EXISTS t_ind_merge_1;

SET enable_analyzer = 1;

CREATE TABLE t_ind_merge_1 (a UInt64, b UInt64, c UInt64, d UInt64, INDEX idx_b b TYPE minmax)
ENGINE = MergeTree
ORDER BY a SETTINGS
    index_granularity = 64,
    index_granularity_bytes = 0,
    merge_max_block_size = 8192,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0;

INSERT INTO t_ind_merge_1 SELECT number, number, rand(), rand() FROM numbers(1000);
INSERT INTO t_ind_merge_1 SELECT number, number, rand(), rand() FROM numbers(1000);

SELECT count() FROM t_ind_merge_1 WHERE b < 100 SETTINGS force_data_skipping_indices = 'idx_b';
EXPLAIN indexes = 1 SELECT count() FROM t_ind_merge_1 WHERE b < 100;

OPTIMIZE TABLE t_ind_merge_1 FINAL;

SELECT count() FROM t_ind_merge_1 WHERE b < 100 SETTINGS force_data_skipping_indices = 'idx_b';
EXPLAIN indexes = 1 SELECT count() FROM t_ind_merge_1 WHERE b < 100;

SYSTEM FLUSH LOGS part_log;

WITH (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND table = 't_ind_merge_1') AS uuid
SELECT
    ProfileEvents['MergedColumns'] AS merged,
    ProfileEvents['GatheredColumns'] AS gathered
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 't_ind_merge_1' AND table_uuid = uuid
    AND event_type = 'MergeParts' AND part_name = 'all_1_2_1' AND error = 0;

DROP TABLE t_ind_merge_1;
