-- Test: column collation plus LowCardinality key column work with vertical insert sorting.
DROP TABLE IF EXISTS t_vi_collate_lc;

CREATE TABLE t_vi_collate_lc
(
    s String COLLATE binary,
    lc LowCardinality(String),
    v UInt8
)
ENGINE = MergeTree
ORDER BY (s, lc)
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_collate_lc VALUES
    ('b', 'x', 2),
    ('a', 'b', 1),
    ('a', 'a', 3);

SELECT s, lc, v FROM t_vi_collate_lc ORDER BY s, lc;

DROP TABLE t_vi_collate_lc;
