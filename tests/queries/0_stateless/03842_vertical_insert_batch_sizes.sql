-- Test: different vertical insert column batch sizes (1 vs 1000).
DROP TABLE IF EXISTS t_vi_batch_1;
DROP TABLE IF EXISTS t_vi_batch_1000;

CREATE TABLE t_vi_batch_1
(
    k UInt64,
    v String
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1,
    vertical_insert_algorithm_columns_batch_size = 1;

CREATE TABLE t_vi_batch_1000
(
    k UInt64,
    v String
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1,
    vertical_insert_algorithm_columns_batch_size = 1000;

INSERT INTO t_vi_batch_1 SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_vi_batch_1000 SELECT number, toString(number) FROM numbers(1000);

SELECT count() FROM t_vi_batch_1;
SELECT count() FROM t_vi_batch_1000;

DROP TABLE t_vi_batch_1;
DROP TABLE t_vi_batch_1000;
