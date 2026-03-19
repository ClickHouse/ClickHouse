-- Test: MATERIALIZED and ALIAS columns are computed in vertical insert.
DROP TABLE IF EXISTS t_vi_materialized_alias;

CREATE TABLE t_vi_materialized_alias
(
    a UInt64,
    b UInt64 MATERIALIZED a * 2,
    c UInt64 ALIAS a + 1
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_materialized_alias (a) SELECT number FROM numbers(3);

SELECT a, b, c
FROM t_vi_materialized_alias
ORDER BY a;

DROP TABLE t_vi_materialized_alias;
