-- Test: Map and Tuple columns are handled with vertical insert.
DROP TABLE IF EXISTS t_vi_map_tuple;

CREATE TABLE t_vi_map_tuple
(
    k UInt64,
    m Map(String, UInt64),
    t Tuple(a UInt64, b String)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_map_tuple
SELECT
    number,
    map('k', number),
    (number, toString(number))
FROM numbers(10);

SELECT sum(m['k']), sum(tupleElement(t, 1))
FROM t_vi_map_tuple;

DROP TABLE t_vi_map_tuple;
