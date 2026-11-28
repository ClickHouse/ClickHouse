-- Test: Nullable columns with many NULLs are handled with vertical insert.
DROP TABLE IF EXISTS t_vi_nullable;

CREATE TABLE t_vi_nullable
(
    x UInt64,
    y Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY x
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_nullable SELECT number, NULL FROM numbers(1000);

SELECT countIf(isNull(y)) FROM t_vi_nullable;

DROP TABLE t_vi_nullable;
