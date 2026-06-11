DROP TABLE IF EXISTS t_ind_merge_2;

CREATE TABLE t_ind_merge_2 (
    a UInt64,
    b UInt64,
    c UInt64,
    d UInt64,
    e UInt64,
    f UInt64,
    INDEX idx_a  a TYPE minmax,
    INDEX idx_b  b TYPE minmax,
    INDEX idx_cd c * d TYPE minmax,
    INDEX idx_d1 d TYPE minmax,
    INDEX idx_d2 d + 7 TYPE set(3),
    INDEX idx_e  e * 3 TYPE set(3))
ENGINE = MergeTree
ORDER BY a SETTINGS
    index_granularity = 64,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0;

INSERT INTO t_ind_merge_2 SELECT number, number, rand(), rand(), rand(), rand() FROM numbers(1000);
INSERT INTO t_ind_merge_2 SELECT number, number, rand(), rand(), rand(), rand() FROM numbers(1000);

OPTIMIZE TABLE t_ind_merge_2 FINAL;
SYSTEM FLUSH LOGS text_log;
SET max_rows_to_read = 0; -- system.text_log can be really big

--- merged: a, c, d; gathered: b, e, f
WITH
    (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND table = 't_ind_merge_2') AS uuid,
    extractAllGroupsVertical(message, 'containing (\\d+) columns \((\\d+) merged, (\\d+) gathered\)')[1] AS groups
SELECT
    groups[1] AS total,
    groups[2] AS merged,
    groups[3] AS gathered
FROM system.text_log
WHERE ((query_id = uuid || '::all_1_2_1') OR (query_id = currentDatabase() || '.t_ind_merge_2::all_1_2_1')) AND notEmpty(groups)
ORDER BY event_time_microseconds;

DROP TABLE t_ind_merge_2;
