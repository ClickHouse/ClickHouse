-- Test: vertical insert writes Nested columns with correct offsets.
DROP TABLE IF EXISTS t_vi_nested;

CREATE TABLE t_vi_nested
(
    id UInt64,
    n Nested(x UInt64, y UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_nested VALUES
    (1, [1, 2], [10, 20]),
    (2, [3], [30]);

SELECT id, n.x, n.y FROM t_vi_nested ORDER BY id;

DROP TABLE t_vi_nested;
