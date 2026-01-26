-- Test: Array key column works with vertical insert.
DROP TABLE IF EXISTS t_vi_array_key;

CREATE TABLE t_vi_array_key
(
    a Array(UInt64),
    v UInt8
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_array_key VALUES
    ([1, 2], 1),
    ([1], 2),
    ([2, 0], 3),
    ([], 4);

SELECT
    sum(length(a)),
    sum(v)
FROM t_vi_array_key;

DROP TABLE t_vi_array_key;
