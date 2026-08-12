SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET use_skip_indexes_on_data_read = 1;

SELECT 'DDL validation';
DROP TABLE IF EXISTS json_index_tokens_invalid_configuration;
CREATE TABLE json_index_tokens_invalid_configuration
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(0))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_index_tokens_invalid_configuration
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(1048577))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_index_tokens_invalid_configuration
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(63 + 1))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE json_index_tokens_invalid_configuration
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues('64'))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_index_tokens_invalid_configuration
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64, 'none'))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT 'LowCardinality analyzer';
DROP TABLE IF EXISTS json_index_tokens_low_cardinality;
CREATE TABLE json_index_tokens_low_cardinality
(
    id UInt64,
    default_data JSON(s LowCardinality(String)),
    one_arg_data JSON(s LowCardinality(String)),
    INDEX default_tokens default_data TYPE text(tokenizer = jsonPathValues) GRANULARITY 1,
    INDEX one_arg_tokens one_arg_data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_index_tokens_low_cardinality VALUES
    (1, '{"s":""}', '{"s":""}'),
    (2, '{"s":"x"}', '{"s":"x"}');
SELECT groupArray(id) FROM json_index_tokens_low_cardinality WHERE one_arg_data.s = 'x'
SETTINGS force_data_skipping_indices = 'one_arg_tokens';
SELECT count() FROM json_index_tokens_low_cardinality WHERE one_arg_data.s = ''
SETTINGS optimize_empty_string_comparisons = 0, force_data_skipping_indices = 'one_arg_tokens'; -- { serverError INDEX_NOT_USED }
DROP TABLE json_index_tokens_low_cardinality;
