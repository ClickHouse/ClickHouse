-- Test: basic vertical insert on a wide table with many columns.
DROP TABLE IF EXISTS t_vi_basic;

CREATE TABLE t_vi_basic
(
    id UInt64,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64,
    c4 UInt64,
    c5 UInt64
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

INSERT INTO t_vi_basic
SELECT
    number AS id,
    number + 1 AS c1,
    number + 2 AS c2,
    number + 3 AS c3,
    number + 4 AS c4,
    number + 5 AS c5
FROM numbers(10);

SELECT count(), sum(id), sum(c5) FROM t_vi_basic;

DROP TABLE t_vi_basic;
