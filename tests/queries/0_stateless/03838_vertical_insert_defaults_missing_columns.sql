-- Test: inserts missing columns use DEFAULT expressions with vertical insert.
DROP TABLE IF EXISTS t_vi_defaults;

CREATE TABLE t_vi_defaults
(
    k UInt64,
    v UInt64 DEFAULT k + 1,
    s String DEFAULT 'x'
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

INSERT INTO t_vi_defaults (k) SELECT number FROM numbers(3);

SELECT k, v, s
FROM t_vi_defaults
ORDER BY k;

DROP TABLE t_vi_defaults;
