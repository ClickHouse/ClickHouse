-- Text index inputs added after insertion are absent in every source part.
-- Their default values must reach index construction in both merge algorithms.
-- An expression can emit tokens even when all physical inputs are NULL.

SELECT 'horizontal nullable';
DROP TABLE IF EXISTS text_missing_0_nullable;
CREATE TABLE text_missing_0_nullable (id UInt64, payload String, spare UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO text_missing_0_nullable VALUES (1, 'alpha', 10), (2, 'beta', 20);
INSERT INTO text_missing_0_nullable VALUES (3, 'alpha', 30), (4, 'beta', 40);
ALTER TABLE text_missing_0_nullable ADD COLUMN s Nullable(String),
    ADD INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;
OPTIMIZE TABLE text_missing_0_nullable FINAL;
SELECT count(), sum(id) FROM text_missing_0_nullable WHERE hasToken(s, 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_0_nullable WHERE hasToken(s, 'sentinel')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
-- Merge the default-only result with a part containing actual text.
INSERT INTO text_missing_0_nullable VALUES (5, 'alpha', 50, 'present'), (6, 'beta', 60, 'present');
OPTIMIZE TABLE text_missing_0_nullable FINAL;
SELECT count(), sum(id) FROM text_missing_0_nullable WHERE hasToken(s, 'present')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_0_nullable WHERE hasToken(s, 'present')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM text_missing_0_nullable WHERE hasToken(s, 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
CHECK TABLE text_missing_0_nullable SETTINGS check_query_single_value_result = 1;
DROP TABLE text_missing_0_nullable;

SELECT 'horizontal expression';
DROP TABLE IF EXISTS text_missing_0_expression;
CREATE TABLE text_missing_0_expression (id UInt64, payload String, spare UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO text_missing_0_expression VALUES (1, 'alpha', 10), (2, 'beta', 20);
INSERT INTO text_missing_0_expression VALUES (3, 'alpha', 30), (4, 'beta', 40);
ALTER TABLE text_missing_0_expression ADD COLUMN s Nullable(String),
    ADD INDEX idx ifNull(s, 'sentinel') TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;
OPTIMIZE TABLE text_missing_0_expression FINAL;
SELECT count(), sum(id) FROM text_missing_0_expression WHERE hasToken(ifNull(s, 'sentinel'), 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_0_expression WHERE hasToken(ifNull(s, 'sentinel'), 'sentinel')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
-- Merge the default-only result with a part containing actual text.
INSERT INTO text_missing_0_expression VALUES (5, 'alpha', 50, 'present'), (6, 'beta', 60, 'present');
OPTIMIZE TABLE text_missing_0_expression FINAL;
SELECT count(), sum(id) FROM text_missing_0_expression WHERE hasToken(ifNull(s, 'sentinel'), 'present')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_0_expression WHERE hasToken(ifNull(s, 'sentinel'), 'present')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM text_missing_0_expression WHERE hasToken(ifNull(s, 'sentinel'), 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
CHECK TABLE text_missing_0_expression SETTINGS check_query_single_value_result = 1;
DROP TABLE text_missing_0_expression;

SELECT 'horizontal two_inputs';
DROP TABLE IF EXISTS text_missing_0_two_inputs;
CREATE TABLE text_missing_0_two_inputs (id UInt64, payload String, spare UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO text_missing_0_two_inputs VALUES (1, 'alpha', 10), (2, 'beta', 20);
INSERT INTO text_missing_0_two_inputs VALUES (3, 'alpha', 30), (4, 'beta', 40);
ALTER TABLE text_missing_0_two_inputs ADD COLUMN s Nullable(String),
    ADD INDEX idx concat(ifNull(s, 'sentinel'), ' ', payload) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;
OPTIMIZE TABLE text_missing_0_two_inputs FINAL;
SELECT count(), sum(id) FROM text_missing_0_two_inputs WHERE hasToken(concat(ifNull(s, 'sentinel'), ' ', payload), 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_0_two_inputs WHERE hasToken(concat(ifNull(s, 'sentinel'), ' ', payload), 'sentinel')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
-- Merge the default-only result with a part containing actual text.
INSERT INTO text_missing_0_two_inputs VALUES (5, 'alpha', 50, 'present'), (6, 'beta', 60, 'present');
OPTIMIZE TABLE text_missing_0_two_inputs FINAL;
SELECT count(), sum(id) FROM text_missing_0_two_inputs WHERE hasToken(concat(ifNull(s, 'sentinel'), ' ', payload), 'present')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_0_two_inputs WHERE hasToken(concat(ifNull(s, 'sentinel'), ' ', payload), 'present')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM text_missing_0_two_inputs WHERE hasToken(concat(ifNull(s, 'sentinel'), ' ', payload), 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
CHECK TABLE text_missing_0_two_inputs SETTINGS check_query_single_value_result = 1;
DROP TABLE text_missing_0_two_inputs;

SELECT 'vertical nullable';
DROP TABLE IF EXISTS text_missing_1_nullable;
CREATE TABLE text_missing_1_nullable (id UInt64, payload String, spare UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO text_missing_1_nullable VALUES (1, 'alpha', 10), (2, 'beta', 20);
INSERT INTO text_missing_1_nullable VALUES (3, 'alpha', 30), (4, 'beta', 40);
ALTER TABLE text_missing_1_nullable ADD COLUMN s Nullable(String),
    ADD INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;
OPTIMIZE TABLE text_missing_1_nullable FINAL;
SELECT count(), sum(id) FROM text_missing_1_nullable WHERE hasToken(s, 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_1_nullable WHERE hasToken(s, 'sentinel')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
-- Merge the default-only result with a part containing actual text.
INSERT INTO text_missing_1_nullable VALUES (5, 'alpha', 50, 'present'), (6, 'beta', 60, 'present');
OPTIMIZE TABLE text_missing_1_nullable FINAL;
SELECT count(), sum(id) FROM text_missing_1_nullable WHERE hasToken(s, 'present')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_1_nullable WHERE hasToken(s, 'present')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM text_missing_1_nullable WHERE hasToken(s, 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
CHECK TABLE text_missing_1_nullable SETTINGS check_query_single_value_result = 1;
DROP TABLE text_missing_1_nullable;

SELECT 'vertical expression';
DROP TABLE IF EXISTS text_missing_1_expression;
CREATE TABLE text_missing_1_expression (id UInt64, payload String, spare UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO text_missing_1_expression VALUES (1, 'alpha', 10), (2, 'beta', 20);
INSERT INTO text_missing_1_expression VALUES (3, 'alpha', 30), (4, 'beta', 40);
ALTER TABLE text_missing_1_expression ADD COLUMN s Nullable(String),
    ADD INDEX idx ifNull(s, 'sentinel') TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;
OPTIMIZE TABLE text_missing_1_expression FINAL;
SELECT count(), sum(id) FROM text_missing_1_expression WHERE hasToken(ifNull(s, 'sentinel'), 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_1_expression WHERE hasToken(ifNull(s, 'sentinel'), 'sentinel')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
-- Merge the default-only result with a part containing actual text.
INSERT INTO text_missing_1_expression VALUES (5, 'alpha', 50, 'present'), (6, 'beta', 60, 'present');
OPTIMIZE TABLE text_missing_1_expression FINAL;
SELECT count(), sum(id) FROM text_missing_1_expression WHERE hasToken(ifNull(s, 'sentinel'), 'present')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_1_expression WHERE hasToken(ifNull(s, 'sentinel'), 'present')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM text_missing_1_expression WHERE hasToken(ifNull(s, 'sentinel'), 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
CHECK TABLE text_missing_1_expression SETTINGS check_query_single_value_result = 1;
DROP TABLE text_missing_1_expression;

SELECT 'vertical two_inputs';
DROP TABLE IF EXISTS text_missing_1_two_inputs;
CREATE TABLE text_missing_1_two_inputs (id UInt64, payload String, spare UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO text_missing_1_two_inputs VALUES (1, 'alpha', 10), (2, 'beta', 20);
INSERT INTO text_missing_1_two_inputs VALUES (3, 'alpha', 30), (4, 'beta', 40);
ALTER TABLE text_missing_1_two_inputs ADD COLUMN s Nullable(String),
    ADD INDEX idx concat(ifNull(s, 'sentinel'), ' ', payload) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;
OPTIMIZE TABLE text_missing_1_two_inputs FINAL;
SELECT count(), sum(id) FROM text_missing_1_two_inputs WHERE hasToken(concat(ifNull(s, 'sentinel'), ' ', payload), 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_1_two_inputs WHERE hasToken(concat(ifNull(s, 'sentinel'), ' ', payload), 'sentinel')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
-- Merge the default-only result with a part containing actual text.
INSERT INTO text_missing_1_two_inputs VALUES (5, 'alpha', 50, 'present'), (6, 'beta', 60, 'present');
OPTIMIZE TABLE text_missing_1_two_inputs FINAL;
SELECT count(), sum(id) FROM text_missing_1_two_inputs WHERE hasToken(concat(ifNull(s, 'sentinel'), ' ', payload), 'present')
    SETTINGS force_data_skipping_indices = 'idx';
SELECT count(), sum(id) FROM text_missing_1_two_inputs WHERE hasToken(concat(ifNull(s, 'sentinel'), ' ', payload), 'present')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(id) FROM text_missing_1_two_inputs WHERE hasToken(concat(ifNull(s, 'sentinel'), ' ', payload), 'sentinel')
    SETTINGS force_data_skipping_indices = 'idx';
CHECK TABLE text_missing_1_two_inputs SETTINGS check_query_single_value_result = 1;
DROP TABLE text_missing_1_two_inputs;
