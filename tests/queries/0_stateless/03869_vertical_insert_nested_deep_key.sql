-- Test: deep nested primary key columns work with vertical insert.
DROP TABLE IF EXISTS t_vi_nested_deep_key;

CREATE TABLE t_vi_nested_deep_key
(
    id UInt64,
    n Nested(
        t Tuple(x UInt64, y UInt64),
        u UInt64
    )
)
ENGINE = MergeTree
ORDER BY (n.t.x, id)
PRIMARY KEY n.t.x
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_nested_deep_key VALUES
    (1, [(2, 20)], [5]),
    (2, [(1, 10)], [7]),
    (3, [(1, 5), (3, 30)], [8, 9]);

SELECT id, n.t.x, n.t.y, n.u
FROM t_vi_nested_deep_key
ORDER BY n.t.x, id;

DROP TABLE t_vi_nested_deep_key;
