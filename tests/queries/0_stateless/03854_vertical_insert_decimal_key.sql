-- Test: Decimal key column works with vertical insert.
DROP TABLE IF EXISTS t_vi_decimal_key;

CREATE TABLE t_vi_decimal_key
(
    d Decimal(18, 4),
    v UInt8
)
ENGINE = MergeTree
ORDER BY d
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_decimal_key VALUES
    (CAST('1.0000' AS Decimal(18, 4)), 1),
    (CAST('2.5000' AS Decimal(18, 4)), 2),
    (CAST('3.2500' AS Decimal(18, 4)), 3);

SELECT
    toInt64(min(d) * 10000),
    toInt64(max(d) * 10000),
    sum(v)
FROM t_vi_decimal_key;

DROP TABLE t_vi_decimal_key;
