-- Test: vertical insert materializes a multi-column skip index on insert.
DROP TABLE IF EXISTS t_vi_skip_multi;

CREATE TABLE t_vi_skip_multi
(
    k UInt64,
    v UInt64,
    INDEX kv_bf (k, v) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    index_granularity = 1,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

SET materialize_skip_indexes_on_insert = 1;

INSERT INTO t_vi_skip_multi
SELECT number, number % 10 FROM numbers(1000);

SELECT count()
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_vi_skip_multi'
  AND active
  AND secondary_indices_uncompressed_bytes > 0;

DROP TABLE t_vi_skip_multi;
