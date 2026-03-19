-- Test: DEFAULT and ALIAS columns used in sort keys are resolved during vertical insert.
DROP TABLE IF EXISTS t_vi_default_alias;

CREATE TABLE t_vi_default_alias
(
    k UInt64,
    ts DateTime,
    d Date DEFAULT toDate(ts),
    s String,
    h UInt64 ALIAS cityHash64(s)
)
ENGINE = MergeTree
ORDER BY (d, k)
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_default_alias (k, ts, s) VALUES
    (1, toDateTime('2020-01-02 00:00:00'), 'aa'),
    (2, toDateTime('2020-01-01 00:00:00'), 'b'),
    (3, toDateTime('2020-01-01 00:00:00'), 'c');

SELECT countIf(h = cityHash64(s)) FROM t_vi_default_alias;

SELECT k, d
FROM t_vi_default_alias
ORDER BY d, k;

DROP TABLE t_vi_default_alias;
