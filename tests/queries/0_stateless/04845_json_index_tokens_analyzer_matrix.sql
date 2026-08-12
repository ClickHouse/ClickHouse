SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS json_index_tokens_analyzer_matrix;
CREATE TABLE json_index_tokens_analyzer_matrix
(
    id UInt64,
    data JSON(
        s String,
        other String,
        n Int64,
        opt Nullable(String),
        tags Array(String)),
    dynamic_data JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX data_tokens data TYPE text(tokenizer = jsonPathValues(48)) GRANULARITY 1,
    INDEX dynamic_tokens dynamic_data TYPE text(tokenizer = jsonPathValues(48)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_index_tokens_analyzer_matrix;
INSERT INTO json_index_tokens_analyzer_matrix VALUES
    (1, '{"s":"Alpha-needle","other":"none","n":10,"opt":"set","tags":["red","green"]}', '{"value":42,"text":"forty two"}'),
    (2, '{"s":"beta-suffix","other":"none","n":20,"tags":["red"]}', '{"value":42.0,"text":42}'),
    (3, concat('{"s":"', repeat('x', 100), '-tail","other":"none","n":30,"tags":["blue"]}'), '{"value":"42","unsupported":[1]}'),
    (4, '{"s":"","other":"none","n":40,"tags":[]}', '{}'),
    (5, '{"s":"unrelated","other":"Alpha-needle","n":50,"tags":["green"]}', '{"value":43}'),
    (6, '{"s":"Äpfel-東京","other":"none","n":60,"tags":["東京"]}', '{"value":-0.0}');

SELECT 'exact predicates';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s = 'Alpha-needle'
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s IN ('Alpha-needle', 'beta-suffix')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE has(data.tags, 'red')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.opt = 'set'
SETTINGS force_data_skipping_indices = 'data_tokens';

SELECT 'patterns';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE startsWith(data.s, 'Alpha-')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE 'beta-%'
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE '%needle%'
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s ILIKE '%ALPHA%'
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE endsWith(data.s, '-tail')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE match(data.s, '^Alpha-.*le$')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE multiSearchAny(data.s, ['needle', 'suffix'])
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE multiSearchAnyUTF8(data.s, ['Äpf', '東京'])
SETTINGS force_data_skipping_indices = 'data_tokens';

SELECT 'path and boolean composition';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.other LIKE '%needle%'
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix PREWHERE data.s = 'Alpha-needle';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s = 'Alpha-needle' AND data.n = 10
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s = 'Alpha-needle' OR data.n = 20
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE NOT data.s = 'Alpha-needle';

SELECT 'dynamic equality';
SET dynamic_throw_on_type_mismatch = 0;
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE dynamic_data.value = 42
SETTINGS force_data_skipping_indices = 'dynamic_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE dynamic_data.value = '42'
SETTINGS force_data_skipping_indices = 'dynamic_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE dynamic_data.value = 0.0
SETTINGS force_data_skipping_indices = 'dynamic_tokens';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE CAST(dynamic_data.value AS String) = '42';

SELECT 'ground truth and read modes';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE '%needle%'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE '%needle%'
SETTINGS query_plan_direct_read_from_text_index = 0;
SET allow_experimental_text_index_lazy_apply = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE '%needle%';

SELECT 'unsupported analyzer forms';
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE data.s = ''
SETTINGS optimize_empty_string_comparisons = 0;
SELECT count() = 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_analyzer_matrix WHERE data.s = ''
    SETTINGS optimize_empty_string_comparisons = 0
)
WHERE position(explain, '__text_index') > 0;
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE CAST(data.n AS String) = '10';
SELECT count() = 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_analyzer_matrix WHERE CAST(data.n AS String) = '10'
)
WHERE position(explain, '__text_index') > 0;
SELECT groupArray(id) FROM json_index_tokens_analyzer_matrix WHERE dynamic_data.unsupported = 1
SETTINGS dynamic_throw_on_type_mismatch = 0, force_data_skipping_indices = 'dynamic_tokens';

SELECT 'supported plans';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_analyzer_matrix WHERE data.s = 'Alpha-needle'
)
WHERE position(explain, '__text_index') > 0;
SELECT count() > 0 FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE '%needle%'
)
WHERE explain LIKE '%data_tokens%';

SYSTEM START MERGES json_index_tokens_analyzer_matrix;
DROP TABLE json_index_tokens_analyzer_matrix;
