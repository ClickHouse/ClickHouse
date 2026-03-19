-- Test: sparse serialization with DEFAULT columns works with vertical insert.
DROP TABLE IF EXISTS t_vi_sparse;

CREATE TABLE t_vi_sparse
(
    x UInt64,
    y UInt64 DEFAULT 0
)
ENGINE = MergeTree
ORDER BY x
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    serialization_info_version = 'with_types',
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_sparse (x) SELECT number FROM numbers(1000);

SELECT column, serialization_kind
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_vi_sparse'
  AND active
ORDER BY column;

DROP TABLE t_vi_sparse;
