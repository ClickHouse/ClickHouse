-- Test: LowCardinality column in ORDER BY works with vertical insert.
DROP TABLE IF EXISTS t_vi_lc_key;

CREATE TABLE t_vi_lc_key
(
    k UInt64,
    lc LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY lc
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_lc_key
SELECT
    number,
    toString(number % 3)
FROM numbers(20);

SELECT
    count(),
    min(lc),
    max(lc),
    countDistinct(lc)
FROM t_vi_lc_key;

DROP TABLE t_vi_lc_key;
