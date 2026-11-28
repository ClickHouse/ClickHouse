-- Test: multiple text indexes are materialized with vertical insert.
SET enable_full_text_index = 1;
SET materialize_skip_indexes_on_insert = 1;

DROP TABLE IF EXISTS t_vi_text_index_multi;

CREATE TABLE t_vi_text_index_multi
(
    id UInt64,
    s String,
    t String,
    INDEX s_idx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX t_idx t TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    index_granularity = 1,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_text_index_multi
SELECT number, toString(number), toString(number * 2) FROM numbers(1000);

SELECT count()
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_vi_text_index_multi'
  AND active
  AND secondary_indices_uncompressed_bytes > 0;

DROP TABLE t_vi_text_index_multi;
