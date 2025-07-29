-- Tests that various conditions are checked during creation of GIN indexes.

-- Using SETTINGS min_bytes_for_full_part_storage = 0 because GIN indexes currently don't work with packed parts

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'Two or less arguments';
CREATE TABLE tab (key UInt64, str String, INDEX inv_idx str TYPE gin('cant_have', 'three', 'args')) ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_full_part_storage = 0; -- { serverError INCORRECT_QUERY }

SELECT '1st argument (tokenizer) must be UInt64';
CREATE TABLE tab (key UInt64, str String, INDEX inv_idx str TYPE gin('string_arg')) ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_full_part_storage = 0; -- { serverError INCORRECT_QUERY }

SELECT '2st argument (max_rows_per_postings_list) must be UInt64';
CREATE TABLE tab (key UInt64, str String, INDEX inv_idx str TYPE gin(1, 'string_arg')) ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_full_part_storage = 0; -- { serverError INCORRECT_QUERY }

SELECT '2st argument (max_rows_per_postings_list) must be bigger than MIN_ROWS_PER_POSTINGS_LIST';
CREATE TABLE tab (key UInt64, str String, INDEX inv_idx str TYPE gin(1, 1)) ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_full_part_storage = 0; -- { serverError INCORRECT_QUERY }

SELECT 'Must be created on single column';
CREATE TABLE tab (key UInt64, str1 String, str2 String, INDEX inv_idx (str1, str2) TYPE gin(1, 9999)) ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_full_part_storage = 0; -- { serverError INCORRECT_NUMBER_OF_COLUMNS }

SELECT 'Must be created on String or FixedString or Array(String) or Array(FixedString) or LowCardinality(String) or LowCardinality(FixedString) columns';
CREATE TABLE tab (key UInt64, str UInt64, INDEX inv_idx str TYPE gin(1, 9999)) ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_full_part_storage = 0; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (key UInt64, str Float32, INDEX inv_idx str TYPE gin(1, 999)) ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_full_part_storage = 0; -- { serverError INCORRECT_QUERY }
CREATE TABLE tab (key UInt64, str Nullable(String), INDEX inv_idx str TYPE gin(1, 999)) ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_full_part_storage = 0; -- { serverError INCORRECT_QUERY }
