-- Test: vertical insert handles INSERT with column order permutation.
DROP TABLE IF EXISTS t_vi_perm;

CREATE TABLE t_vi_perm
(
    id UInt64,
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_perm VALUES (3, 30, 300), (1, 10, 100), (2, 20, 200);

SELECT id, a, b FROM t_vi_perm ORDER BY id;

DROP TABLE t_vi_perm;
