-- Test: checksums match between vertical and horizontal insert paths.
DROP TABLE IF EXISTS t_vi_checksums_v;
DROP TABLE IF EXISTS t_vi_checksums_h;

CREATE TABLE t_vi_checksums_v
(
    k UInt64,
    v String
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

CREATE TABLE t_vi_checksums_h
(
    k UInt64,
    v String
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 0,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_checksums_v SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_vi_checksums_h SELECT number, toString(number) FROM numbers(1000);

SELECT
    (SELECT sum(sipHash64(k, v)) FROM t_vi_checksums_v)
  =
    (SELECT sum(sipHash64(k, v)) FROM t_vi_checksums_h);

DROP TABLE t_vi_checksums_v;
DROP TABLE t_vi_checksums_h;
