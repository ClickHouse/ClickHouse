-- Test: LowCardinality(Nullable) key column works with vertical insert.
DROP TABLE IF EXISTS t_vi_lc_nullable_key;

CREATE TABLE t_vi_lc_nullable_key
(
    lc LowCardinality(Nullable(String)),
    v UInt8
)
ENGINE = MergeTree
ORDER BY lc
SETTINGS
    allow_nullable_key = 1,
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_lc_nullable_key VALUES
    ('a', 1),
    (NULL, 2),
    ('b', 3),
    (NULL, 4),
    ('a', 5);

SELECT
    count(),
    countIf(isNull(lc)),
    min(lc),
    max(lc),
    countDistinctIf(lc, isNotNull(lc))
FROM t_vi_lc_nullable_key;

DROP TABLE t_vi_lc_nullable_key;
