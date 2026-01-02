-- Test: Nested subcolumns in ORDER BY work with vertical insert.
DROP TABLE IF EXISTS t_vi_nested_sort_key;

CREATE TABLE t_vi_nested_sort_key
(
    id UInt64,
    n Nested(x UInt64, y UInt64)
)
ENGINE = MergeTree
ORDER BY n.x
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_nested_sort_key VALUES
    (1, [2], [20]),
    (2, [1], [10]),
    (3, [1, 2], [10, 20]);

SELECT id, n.x, n.y FROM t_vi_nested_sort_key ORDER BY n.x, id;

DROP TABLE t_vi_nested_sort_key;
