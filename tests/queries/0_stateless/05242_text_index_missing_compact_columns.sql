-- A late-added text-index input must stay absent from Compact output columns.
-- The same omission must hold when index materialization is disabled.

SELECT 'materialize_skip_indexes_on_merge = 1';
DROP TABLE IF EXISTS text_missing_compact_1;
CREATE TABLE text_missing_compact_1 (id UInt64, payload String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '1Gi', min_rows_for_wide_part = 1000000,
    materialize_skip_indexes_on_merge = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO text_missing_compact_1 VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO text_missing_compact_1 VALUES (3, 'alpha'), (4, 'beta');
ALTER TABLE text_missing_compact_1 ADD COLUMN s Nullable(String),
    ADD INDEX idx ifNull(s, 'sentinel') TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;
OPTIMIZE TABLE text_missing_compact_1 FINAL;
SELECT part_type, rows FROM system.parts
WHERE database = currentDatabase() AND table = 'text_missing_compact_1' AND active;
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'text_missing_compact_1' AND active AND column = 's';
SELECT count(), sum(id) FROM text_missing_compact_1 WHERE hasToken(ifNull(s, 'sentinel'), 'sentinel')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM text_missing_compact_1 WHERE hasToken(ifNull(s, 'sentinel'), 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
CHECK TABLE text_missing_compact_1 SETTINGS check_query_single_value_result = 1;
-- Mix absent inputs with actual values, then verify the rebuilt index and scan agree.
INSERT INTO text_missing_compact_1 VALUES (5, 'alpha', 'present'), (6, 'beta', 'present');
OPTIMIZE TABLE text_missing_compact_1 FINAL;
SELECT count(), sum(id) FROM text_missing_compact_1 WHERE hasToken(ifNull(s, 'sentinel'), 'present')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM text_missing_compact_1 WHERE hasToken(ifNull(s, 'sentinel'), 'present')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_compact_1 WHERE hasToken(ifNull(s, 'sentinel'), 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
CHECK TABLE text_missing_compact_1 SETTINGS check_query_single_value_result = 1;
DROP TABLE text_missing_compact_1;

SELECT 'materialize_skip_indexes_on_merge = 0';
DROP TABLE IF EXISTS text_missing_compact_0;
CREATE TABLE text_missing_compact_0 (id UInt64, payload String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '1Gi', min_rows_for_wide_part = 1000000,
    materialize_skip_indexes_on_merge = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO text_missing_compact_0 VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO text_missing_compact_0 VALUES (3, 'alpha'), (4, 'beta');
ALTER TABLE text_missing_compact_0 ADD COLUMN s Nullable(String),
    ADD INDEX idx ifNull(s, 'sentinel') TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;
OPTIMIZE TABLE text_missing_compact_0 FINAL;
SELECT part_type, rows FROM system.parts
WHERE database = currentDatabase() AND table = 'text_missing_compact_0' AND active;
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'text_missing_compact_0' AND active AND column = 's';
SELECT count(), sum(id) FROM text_missing_compact_0 WHERE hasToken(ifNull(s, 'sentinel'), 'sentinel')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT secondary_indices_compressed_bytes FROM system.parts
WHERE database = currentDatabase() AND table = 'text_missing_compact_0' AND active;
CHECK TABLE text_missing_compact_0 SETTINGS check_query_single_value_result = 1;
-- Mix absent inputs with actual values, then verify the rebuilt index and scan agree.
INSERT INTO text_missing_compact_0 VALUES (5, 'alpha', 'present'), (6, 'beta', 'present');
OPTIMIZE TABLE text_missing_compact_0 FINAL;
SELECT count(), sum(id) FROM text_missing_compact_0 WHERE hasToken(ifNull(s, 'sentinel'), 'present')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
CHECK TABLE text_missing_compact_0 SETTINGS check_query_single_value_result = 1;
DROP TABLE text_missing_compact_0;
