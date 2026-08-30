SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_index_tokens_dynamic_cast;
CREATE TABLE json_index_tokens_dynamic_cast
(
    id UInt64,
    data JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(32)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_index_tokens_dynamic_cast VALUES
    (1, '{"value":42}'),
    (2, '{"value":"42"}'),
    (3, '{"value":43}'),
    (4, '{"value":[1,2,3,4,5]}'),
    (5, '{}');

SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic_cast WHERE CAST(data.value AS String) = '42'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic_cast WHERE CAST(data.value AS String) = '[1,2,3,4,5]'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic_cast WHERE CAST(data.value AS String) = '__missing__'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic_cast WHERE CAST(data.value AS String) = '42'
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_dynamic_cast WHERE CAST(data.value AS String) = '42'
)
WHERE position(explain, '__text_index') > 0;

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_dynamic_cast WHERE toString(data.value) = '42'
)
WHERE position(explain, '__text_index') > 0;

DROP TABLE json_index_tokens_dynamic_cast;

DROP TABLE IF EXISTS json_path_values_special_values;
CREATE TABLE json_path_values_special_values
(
    id UInt64,
    data JSON(s String, tags Array(Nullable(String))),
    dynamic JSON(max_dynamic_paths = 16, max_dynamic_types = 16),
    shared JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX data_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX dynamic_tokens dynamic TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX shared_tokens shared TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_path_values_special_values;
INSERT INTO json_path_values_special_values VALUES
    (1, '{"s":"","tags":["",null,"x"]}', '{"s":"","tags":["",null,"x"]}', '{"s":"","tags":["",null,"x"]}'),
    (2, '{"s":"x","tags":["x"]}', '{"s":"x","tags":["x"]}', '{"s":"x","tags":["x"]}'),
    (3, '{}', '{}', '{}');

SELECT arraySort(groupArray(id)) FROM json_path_values_special_values WHERE data.s = ''
SETTINGS optimize_empty_string_comparisons = 0;
SELECT count() FROM json_path_values_special_values WHERE data.s = ''
SETTINGS optimize_empty_string_comparisons = 0, force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id)) FROM json_path_values_special_values WHERE has(data.tags, '');
SELECT count() FROM json_path_values_special_values WHERE has(data.tags, '')
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id)) FROM json_path_values_special_values
WHERE has(data.tags, CAST(NULL, 'Nullable(String)'));
SELECT count() FROM json_path_values_special_values
WHERE has(data.tags, CAST(NULL, 'Nullable(String)'))
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id)) FROM json_path_values_special_values WHERE dynamic.s = ''
SETTINGS optimize_empty_string_comparisons = 0;
SELECT count() FROM json_path_values_special_values WHERE dynamic.s = ''
SETTINGS optimize_empty_string_comparisons = 0, force_data_skipping_indices = 'dynamic_tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id)) FROM json_path_values_special_values WHERE shared.s = ''
SETTINGS optimize_empty_string_comparisons = 0;
SELECT count() FROM json_path_values_special_values WHERE shared.s = ''
SETTINGS optimize_empty_string_comparisons = 0, force_data_skipping_indices = 'shared_tokens'; -- { serverError INDEX_NOT_USED }

SYSTEM START MERGES json_path_values_special_values;
OPTIMIZE TABLE json_path_values_special_values FINAL;

SELECT arraySort(groupArray(id)) FROM json_path_values_special_values WHERE has(data.tags, '');
SELECT count() FROM json_path_values_special_values WHERE has(data.tags, '')
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_path_values_special_values;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS json_index_tokens_ifnull;
CREATE TABLE json_index_tokens_ifnull
(
    id UInt64,
    data JSON(s Nullable(String)),
    dynamic_data JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX dynamic_tokens dynamic_data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_index_tokens_ifnull;
INSERT INTO json_index_tokens_ifnull VALUES
    (1, '{"s":"mcp"}', '{"value":42}'),
    (2, '{"s":"other"}', '{"value":"42"}'),
    (3, '{"s":""}', '{"value":43}'),
    (4, '{"s":null}', '{"value":null}'),
    (5, '{}', '{}');

SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') = 'mcp';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE 'mcp' = ifNull(data.s, '');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE coalesce(data.s, '') = 'mcp';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') LIKE '%other%';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') ILIKE '%OTHER%';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE match(ifNull(data.s, ''), '^mcp$');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE ifNull(toString(dynamic_data.value), '') = '42';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE ifNull(dynamic_data.value, '') = '42';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') = 'mcp'
    SETTINGS query_plan_optimize_count_from_text_index = 0
)
WHERE position(explain, '__text_index') > 0;

SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE ifNull(data.s, '%') LIKE '%';

SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE ifNull(data.s, 'mcp') = 'mcp';

SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE in(ifNull(data.s, ''), tuple('mcp', 'other'));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE globalIn(coalesce(data.s, ''), tuple('mcp', 'other'));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE in(nullIf(nullIf(ifNull(data.s, ''), ''), 'null'), ['mcp', 'other']);
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE in(ifNull(toString(dynamic_data.value), ''), tuple('42', '43'));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE has(['mcp', 'other'], ifNull(data.s, ''));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE has(['42'], ifNull(toString(dynamic_data.value), ''));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE match(nullIf(nullIf(ifNull(data.s, ''), ''), 'null'), '^other$');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE in(toString(nullIf(nullIf(ifNull(data.s, ''), ''), 'null')), ['mcp']);

SELECT count() > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE match(nullIf(nullIf(ifNull(data.s, ''), ''), 'null'), '^other$')
)
WHERE explain LIKE '%Name: tokens%';

SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE in(ifNull(data.s, ''), tuple('', 'mcp'));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE in(nullIf(data.s, 'mcp'), tuple('mcp'));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE has([''], ifNull(data.s, ''));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_ifnull WHERE match(nullIf(data.s, 'other'), '^other$');

SELECT count() = 0
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE match(nullIf(data.s, 'other'), '^other$')
)
WHERE explain LIKE '%Name: tokens%';

DROP TABLE json_index_tokens_ifnull;

DROP TABLE IF EXISTS json_path_values_dynamic_arrays;
CREATE TABLE json_path_values_dynamic_arrays
(
    id UInt64,
    data JSON(max_dynamic_paths = 16, max_dynamic_types = 16),
    shared JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX data_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX shared_tokens shared TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_path_values_dynamic_arrays;
INSERT INTO json_path_values_dynamic_arrays VALUES
    (1,
     '{"tags":["foo",""],"cast_tags":["1","2"],"mixed":["foo"]}',
     '{"tags":["foo",""]}'),
    (2,
     concat('{"tags":["bar","', repeat('x', 100), '"],"cast_tags":[1,2],"mixed":"not-an-array"}'),
     '{"tags":["bar"]}'),
    (3,
     '{"tags":[1,2],"cast_tags":["3"],"mixed":["bar"]}',
     '{"tags":[1,2]}'),
    (4,
     '{"tags":[null,"foo"]}',
     '{"tags":[null,"foo"]}'),
    (5, '{}', '{}');

SELECT 'exact dynamic array types';
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, 'foo')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, '');
SELECT count() FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, '')
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, repeat('x', 100))
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(Int64))`, 2)
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, CAST(NULL, 'Nullable(String)'));
SELECT count() FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, CAST(NULL, 'Nullable(String)'))
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'shared dynamic arrays';
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(shared.tags.:`Array(Nullable(String))`, 'foo')
SETTINGS force_data_skipping_indices = 'shared_tokens';

SELECT 'converting casts';
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.cast_tags AS Array(Nullable(String))), '1')
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.cast_tags AS Array(Nullable(String))), '1')
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.cast_tags AS Array(Nullable(Int64))), 2)
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.mixed AS Array(Nullable(String))), 'foo')
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }
SELECT count() FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.mixed AS Array(Nullable(String))), 'foo')
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'planner';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_path_values_dynamic_arrays
    WHERE has(data.tags.:`Array(Nullable(String))`, 'foo')
    SETTINGS query_plan_optimize_count_from_text_index = 0
)
WHERE position(explain, '__text_index') > 0;
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_path_values_dynamic_arrays
    WHERE has(CAST(data.cast_tags AS Array(Nullable(String))), '1')
)
WHERE position(explain, '__text_index') > 0;

SYSTEM START MERGES json_path_values_dynamic_arrays;
OPTIMIZE TABLE json_path_values_dynamic_arrays FINAL;

SELECT 'merged';
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(data.tags.:`Array(Nullable(String))`, 'foo')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_dynamic_arrays
WHERE has(CAST(data.cast_tags AS Array(Nullable(String))), '1')
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE json_path_values_dynamic_arrays;
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

SELECT 'Dynamic string patterns';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic
WHERE multiSearchAny(shared_path.text, ['42'])
SETTINGS force_data_skipping_indices = 'shared_path_tokens', text_index_like_min_pattern_length = 2;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_dynamic
WHERE multiSearchAnyUTF8(shared_path.text, ['43'])
SETTINGS force_data_skipping_indices = 'shared_path_tokens', text_index_like_min_pattern_length = 2;

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
SELECT count() FROM json_index_tokens_dynamic WHERE shared_path.unsupported = 1
SETTINGS dynamic_throw_on_type_mismatch = 0, force_data_skipping_indices = 'shared_path_tokens',
    query_plan_optimize_count_from_text_index = 1;

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
