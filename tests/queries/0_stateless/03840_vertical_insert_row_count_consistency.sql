-- Test: row count consistency between vertical insert and table metadata.
DROP TABLE IF EXISTS t_vi_rowcount;

CREATE TABLE t_vi_rowcount
(
    k UInt64,
    a UInt64,
    b String
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

INSERT INTO t_vi_rowcount SELECT number, number * 2, toString(number) FROM numbers(500);

SELECT countDistinct(rows)
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_vi_rowcount'
  AND active;

DROP TABLE t_vi_rowcount;
