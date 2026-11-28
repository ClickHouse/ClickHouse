-- Test: Nullable key column works with vertical insert (allow_nullable_key).
DROP TABLE IF EXISTS t_vi_nullable_key;

CREATE TABLE t_vi_nullable_key
(
    x Nullable(Int64),
    v UInt8
)
ENGINE = MergeTree
ORDER BY x
SETTINGS
    allow_nullable_key = 1,
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_nullable_key VALUES
    (NULL, 1),
    (2, 2),
    (1, 3),
    (NULL, 4);

SELECT
    count(),
    countIf(isNull(x)),
    min(x),
    max(x)
FROM t_vi_nullable_key;

DROP TABLE t_vi_nullable_key;
