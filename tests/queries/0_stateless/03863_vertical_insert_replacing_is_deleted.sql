-- Test: ReplacingMergeTree with version/is_deleted columns works with vertical insert and FINAL.
DROP TABLE IF EXISTS t_vi_replacing_deleted;

CREATE TABLE t_vi_replacing_deleted
(
    k UInt64,
    v UInt64,
    version UInt64,
    is_deleted UInt8
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_replacing_deleted VALUES
    (1, 10, 1, 0),
    (1, 20, 2, 1),
    (2, 30, 1, 0);

SELECT count(), sum(v) FROM t_vi_replacing_deleted FINAL;

DROP TABLE t_vi_replacing_deleted;
