-- Test: multiple Nested columns plus regular key work with vertical insert.
DROP TABLE IF EXISTS t_vi_multi_nested_sort_key;

CREATE TABLE t_vi_multi_nested_sort_key
(
    id UInt64,
    k UInt64,
    n1 Nested(x UInt64, y UInt64),
    n2 Nested(x UInt64, y UInt64)
)
ENGINE = MergeTree
ORDER BY (n1.x, n2.y, k)
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_multi_nested_sort_key VALUES
    (1, 2, [2], [20], [5], [50]),
    (2, 1, [1], [10], [7], [70]),
    (3, 1, [1], [11], [3], [30]),
    (4, 0, [1], [10], [3], [20]);

SELECT id, k, n1.x, n1.y, n2.x, n2.y
FROM t_vi_multi_nested_sort_key
ORDER BY n1.x, n2.y, k;

DROP TABLE t_vi_multi_nested_sort_key;
