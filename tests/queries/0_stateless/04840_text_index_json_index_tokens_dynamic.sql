SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;
SET dynamic_throw_on_type_mismatch = 0;

DROP TABLE IF EXISTS json_index_tokens_dynamic;
CREATE TABLE json_index_tokens_dynamic
(
    id UInt64,
    shared_path JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    shared_variant JSON(max_dynamic_paths = 16, max_dynamic_types = 0),
    typed JSON(s String),
    INDEX shared_path_tokens shared_path TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX shared_variant_tokens shared_variant TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX typed_tokens typed TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_index_tokens_dynamic;
INSERT INTO json_index_tokens_dynamic VALUES
    (1,
     concat('{"number":42,"text":"42","empty":"","zero":0,"boundary":9007199254740992,"uint":18446744073709551615,"bool":true,"long":"', repeat('a', 100), '","unsupported":[1]}'),
     '{"number":42,"text":"42","empty":""}',
     '{"s":""}');
INSERT INTO json_index_tokens_dynamic VALUES
    (2,
     concat('{"number":42.0,"text":42,"empty":"x","zero":-0.0,"boundary":9007199254740993,"uint":18446744073709551614,"bool":1,"long":"', repeat('a', 99), 'b"}'),
     '{"number":42.0,"text":42,"empty":"x"}',
     '{"s":"x"}');
INSERT INTO json_index_tokens_dynamic VALUES
    (3, '{"number":43,"text":"43","zero":1,"boundary":9007199254740992.0,"bool":1.0}', '{"number":43,"text":"43"}', '{}');
INSERT INTO json_index_tokens_dynamic VALUES
    (4, '{}', '{}', '{}');

SELECT 'JSON shared paths';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.number = 42
SETTINGS force_data_skipping_indices = 'shared_path_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.number = 42.0
SETTINGS force_data_skipping_indices = 'shared_path_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.text = '42'
SETTINGS force_data_skipping_indices = 'shared_path_tokens';

SELECT 'Dynamic shared variant';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_variant.number = 42
SETTINGS force_data_skipping_indices = 'shared_variant_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_variant.number = 42.0
SETTINGS force_data_skipping_indices = 'shared_variant_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_variant.text = '42'
SETTINGS force_data_skipping_indices = 'shared_variant_tokens';

SELECT 'Dynamic numeric edges';
SELECT arraySort(groupArrayIf(id, shared_path.zero = 0.0)) FROM json_index_tokens_dynamic;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.zero = 0.0
SETTINGS force_data_skipping_indices = 'shared_path_tokens';
SELECT arraySort(groupArrayIf(id, shared_path.boundary = 9007199254740992.0)) FROM json_index_tokens_dynamic;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.boundary = 9007199254740992.0
SETTINGS force_data_skipping_indices = 'shared_path_tokens';
SELECT arraySort(groupArrayIf(id, shared_path.boundary = toUInt64('9007199254740993'))) FROM json_index_tokens_dynamic;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.boundary = toUInt64('9007199254740993')
SETTINGS force_data_skipping_indices = 'shared_path_tokens';
SELECT arraySort(groupArrayIf(id, shared_path.uint = toUInt64('18446744073709551615'))) FROM json_index_tokens_dynamic;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.uint = toUInt64('18446744073709551615')
SETTINGS force_data_skipping_indices = 'shared_path_tokens';
SELECT arraySort(groupArrayIf(id, shared_path.bool = true)) FROM json_index_tokens_dynamic;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.bool = true
SETTINGS force_data_skipping_indices = 'shared_path_tokens';

SELECT 'Dynamic truncated equality';
SELECT arraySort(groupArrayIf(id, shared_path.long = repeat('a', 100))) FROM json_index_tokens_dynamic;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.long = repeat('a', 100)
SETTINGS force_data_skipping_indices = 'shared_path_tokens';

SELECT 'Dynamic unsupported type';
SELECT count() FROM json_index_tokens_dynamic WHERE shared_path.unsupported = 1
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0,
    dynamic_throw_on_type_mismatch = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM json_index_tokens_dynamic WHERE shared_path.unsupported = 1
SETTINGS force_data_skipping_indices = 'shared_path_tokens',
    dynamic_throw_on_type_mismatch = 1; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.unsupported = 1
SETTINGS dynamic_throw_on_type_mismatch = 0, force_data_skipping_indices = 'shared_path_tokens';

SELECT 'empty strings';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.empty = '';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_variant.empty = '';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE typed.s = ''
SETTINGS optimize_empty_string_comparisons = 0;

SELECT 'empty string plan';
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_dynamic WHERE typed.s = ''
    SETTINGS optimize_empty_string_comparisons = 0
)
WHERE position(explain, '__text_index') > 0;

SYSTEM START MERGES json_index_tokens_dynamic;
OPTIMIZE TABLE json_index_tokens_dynamic FINAL;

SELECT 'merged part';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_path.number = 42;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic WHERE shared_variant.number = 42.0;

DROP TABLE json_index_tokens_dynamic;
