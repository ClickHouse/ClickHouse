-- Test: TTL expressions are applied correctly with vertical insert.
DROP TABLE IF EXISTS t_vi_ttl;

CREATE TABLE t_vi_ttl
(
    id UInt64,
    d Date,
    payload String
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_ttl VALUES
    (1, today() + 7, 'a'),
    (2, today() + 8, 'b');

SELECT count()
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_vi_ttl'
  AND active
  AND delete_ttl_info_min > 0
  AND delete_ttl_info_max > 0;

DROP TABLE t_vi_ttl;
