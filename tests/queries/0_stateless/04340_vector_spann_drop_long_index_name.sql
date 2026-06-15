-- Tags: no-fasttest, no-ordinary-database, no-random-merge-tree-settings

-- Regression for DROP INDEX on vector_spann with hashed skip-index filenames.
-- vector_spann has a second substream (.pl.idx); DROP INDEX must remove hashed posting-list files too.

SET allow_experimental_vector_spann_index = 1;
SET mutations_sync = 1;
SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS tab_spann_drop_long_idx;

CREATE TABLE tab_spann_drop_long_idx
(
    id Int32,
    vec Array(Float32),
    INDEX `explicit_vector_spann_index_with_an_extremely_long_name_that_exceeds_the_maximum_file_name_length` vec
        TYPE vector_spann('spann', 'L2Distance', 2, 'bf16', 32, 128, 1.0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 4,
    replace_long_file_name_to_hash = 1,
    max_file_name_length = 50,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO tab_spann_drop_long_idx
SELECT number, [toFloat32(number), toFloat32(number + 1)]
FROM numbers(16);

ALTER TABLE tab_spann_drop_long_idx
    DROP INDEX `explicit_vector_spann_index_with_an_extremely_long_name_that_exceeds_the_maximum_file_name_length`;

CHECK TABLE tab_spann_drop_long_idx;

SELECT count() FROM tab_spann_drop_long_idx;

DROP TABLE tab_spann_drop_long_idx;
