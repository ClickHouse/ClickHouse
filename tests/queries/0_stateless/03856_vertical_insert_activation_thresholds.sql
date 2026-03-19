-- Test: activation thresholds (min_rows/min_columns/min_bytes) and on/off detection via query_log + parts_columns.
DROP TABLE IF EXISTS t_vi_activation_on;
DROP TABLE IF EXISTS t_vi_activation_off;
SET async_insert = 0;

CREATE TABLE t_vi_activation_on
(
    k UInt64,
    v UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 50,
    vertical_insert_algorithm_min_bytes_to_activate = 1,
    vertical_insert_algorithm_min_columns_to_activate = 2;

INSERT INTO t_vi_activation_on
SELECT number, number * 2, toString(number)
FROM numbers(100);

CREATE TABLE t_vi_activation_off
(
    k UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1000,
    vertical_insert_algorithm_min_bytes_to_activate = 1,
    vertical_insert_algorithm_min_columns_to_activate = 5;

INSERT INTO t_vi_activation_off
SELECT number, toString(number)
FROM numbers(10);

SYSTEM FLUSH LOGS query_log;

SELECT ifNull(max(ProfileEvents['VerticalInsertRows']) > 0, 0)
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE 'INSERT INTO t_vi_activation_on%';

SELECT ifNull(max(ProfileEvents['VerticalInsertRows']) > 0, 0)
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE 'INSERT INTO t_vi_activation_off%';

SELECT countDistinct(name) > 0
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_vi_activation_on'
  AND active;

DROP TABLE t_vi_activation_on;
DROP TABLE t_vi_activation_off;
