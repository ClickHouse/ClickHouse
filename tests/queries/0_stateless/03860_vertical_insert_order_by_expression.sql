-- Test: ORDER BY expressions (not just plain columns) are respected with vertical insert.
DROP TABLE IF EXISTS t_vi_order_expr;

CREATE TABLE t_vi_order_expr
(
    k UInt64,
    s String,
    ts DateTime
)
ENGINE = MergeTree
ORDER BY (toDate(ts), length(s), k)
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_order_expr VALUES
    (1, 'a', toDateTime('2020-01-02 00:00:00')),
    (2, 'aa', toDateTime('2020-01-01 00:00:00')),
    (3, 'b', toDateTime('2020-01-01 00:00:00'));

SELECT k, toDate(ts), length(s)
FROM t_vi_order_expr
ORDER BY (toDate(ts), length(s), k);

DROP TABLE t_vi_order_expr;
