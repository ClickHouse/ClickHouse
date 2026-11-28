-- Test: LowCardinality + Nullable columns are handled with vertical insert.
DROP TABLE IF EXISTS t_vi_lc;

CREATE TABLE t_vi_lc
(
    x UInt64,
    y LowCardinality(String),
    z Nullable(UInt64)
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

INSERT INTO t_vi_lc
SELECT
    number,
    toString(number % 10),
    if(number % 3 = 0, NULL, number)
FROM numbers(1000);

SELECT
    count(),
    countDistinct(y),
    countIf(isNull(z))
FROM t_vi_lc;

DROP TABLE t_vi_lc;
