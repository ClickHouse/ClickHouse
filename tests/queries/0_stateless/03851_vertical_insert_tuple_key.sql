-- Test: Tuple key column works with vertical insert.
DROP TABLE IF EXISTS t_vi_tuple_key;

CREATE TABLE t_vi_tuple_key
(
    t Tuple(UInt64, String),
    v UInt8
)
ENGINE = MergeTree
ORDER BY t
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_tuple_key VALUES
    ((1, 'a'), 1),
    ((2, 'b'), 2),
    ((3, 'c'), 3);

SELECT
    sum(tupleElement(t, 1)),
    sum(v),
    min(tupleElement(t, 2)),
    max(tupleElement(t, 2))
FROM t_vi_tuple_key;

DROP TABLE t_vi_tuple_key;
