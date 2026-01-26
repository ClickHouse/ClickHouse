-- Test: Map key column works with vertical insert.
DROP TABLE IF EXISTS t_vi_map_key;

CREATE TABLE t_vi_map_key
(
    m Map(String, UInt64),
    v UInt8
)
ENGINE = MergeTree
ORDER BY m
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_map_key VALUES
    (map('a', 1), 1),
    (map('b', 2), 2),
    (map('a', 3, 'b', 4), 3);

SELECT
    sum(m['a']),
    sum(m['b']),
    count()
FROM t_vi_map_key;

DROP TABLE t_vi_map_key;
