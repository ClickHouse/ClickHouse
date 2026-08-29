SET enable_json_type = 1;
SET enable_analyzer = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_path_values_in_fallback;
CREATE TABLE json_path_values_in_fallback
(
    id UInt64,
    data JSON(s String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_in_fallback VALUES
    (1, '{"s":"alpha"}'),
    (2, '{"s":"beta"}');
ALTER TABLE json_path_values_in_fallback
    ADD INDEX tokens data TYPE text(tokenizer = jsonPathValues(32)) GRANULARITY 1;
INSERT INTO json_path_values_in_fallback VALUES
    (3, '{"s":"alpha"}'),
    (4, '{"s":"gamma"}');

SELECT arraySort(groupArray(id))
FROM json_path_values_in_fallback
WHERE data.s IN ('alpha', 'gamma')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM json_path_values_in_fallback WHERE data.s IN ('alpha', 'gamma')
)
WHERE position(explain, '__text_index') > 0;

DROP TABLE IF EXISTS json_path_values_long_in;
CREATE TABLE json_path_values_long_in
(
    id UInt64,
    data JSON(s String),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(32)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_long_in
VALUES (7, concat('{"s":"', repeat('x', 200), '"}'));

SELECT arraySort(groupArray(id))
FROM json_path_values_long_in
WHERE data.s IN (repeat('x', 200))
SETTINGS force_data_skipping_indices = 'tokens';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM json_path_values_long_in WHERE data.s IN (repeat('x', 200))
)
WHERE position(explain, '__text_index') > 0;

DROP TABLE IF EXISTS json_path_values_map_keys;
CREATE TABLE json_path_values_map_keys
(
    id UInt64,
    data JSON(m Map(String, String)),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_map_keys VALUES
    (1, '{"m":{"a":"x"}}'),
    (2, '{"m":{"b":"y"}}');

SELECT arraySort(groupArray(id))
FROM json_path_values_map_keys
WHERE data.m.keys = ['a'];

SELECT count()
FROM
(
    EXPLAIN indexes = 1
    SELECT * FROM json_path_values_map_keys WHERE data.m.keys = ['a']
)
WHERE explain LIKE '%Name: tokens%';

DROP TABLE IF EXISTS json_path_values_dynamic_exception;
CREATE TABLE json_path_values_dynamic_exception
(
    id UInt64,
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_dynamic_exception FORMAT JSONEachRow
{"id":1,"data":{"value":1.5}}
{"id":2,"data":{"value":2}}
{"id":3,"data":{"value":"1.5"}}
{"id":4,"data":{"value":-0.0}}

SELECT arraySort(groupArray(id))
FROM json_path_values_dynamic_exception
WHERE data.value = '1.5'
SETTINGS use_skip_indexes = 0,
    query_plan_direct_read_from_text_index = 0,
    use_skip_indexes_on_data_read = 0; -- { serverError TYPE_MISMATCH }

SELECT arraySort(groupArray(id))
FROM json_path_values_dynamic_exception
WHERE data.value = '1.5'; -- { serverError TYPE_MISMATCH }

SELECT arraySort(groupArray(id))
FROM json_path_values_dynamic_exception
WHERE data.value = '1.5'
SETTINGS dynamic_throw_on_type_mismatch = 0,
    force_data_skipping_indices = 'tokens';

DROP TABLE json_path_values_dynamic_exception;
DROP TABLE json_path_values_map_keys;
DROP TABLE json_path_values_long_in;
DROP TABLE json_path_values_in_fallback;

DROP TABLE IF EXISTS json_path_values_review_predicates;
CREATE TABLE json_path_values_review_predicates
(
    id UInt64,
    data JSON(k Nullable(String), fixed Nullable(FixedString(3))),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_review_predicates VALUES
    (1, '{"k":"v3"}'),
    (2, '{"k":"x","fixed":"abc"}'),
    (3, '{}');

SELECT 'FixedString set fallback';
SELECT arraySort(groupArray(id))
FROM json_path_values_review_predicates
WHERE ifNull(data.fixed, 'a') IN (SELECT toFixedString('a', 3));
SELECT count()
FROM json_path_values_review_predicates
WHERE ifNull(data.fixed, 'a') IN (SELECT toFixedString('a', 3))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'projected nullable predicate';
SELECT id, data.k = 'v3'
FROM json_path_values_review_predicates
WHERE data.k = 'v3' OR id >= 0
ORDER BY id;
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT id, data.k = 'v3'
    FROM json_path_values_review_predicates
    WHERE data.k = 'v3' OR id >= 0
)
WHERE position(explain, '__text_index') > 0;
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM json_path_values_review_predicates
    WHERE data.k = 'v3'
)
WHERE position(explain, '__text_index') > 0;

DROP TABLE json_path_values_review_predicates;

DROP TABLE IF EXISTS json_path_values_review_map;
CREATE TABLE json_path_values_review_map
(
    id UInt64,
    data JSON(attrs Map(String, String)),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64), dictionary_block_size = 2) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_review_map
SELECT number, format('{{"attrs":{{"k":"v{0}"}}}}', number)
FROM numbers(8);

SELECT 'map key pruning';
SELECT count() = 1
FROM
(
    EXPLAIN indexes = 1
    SELECT count()
    FROM json_path_values_review_map
    WHERE mapContains(data.attrs, 'missing')
)
WHERE explain LIKE '%Granules: 0/8%';
SELECT count()
FROM json_path_values_review_map
WHERE mapContains(data.attrs, 'missing')
SETTINGS force_data_skipping_indices = 'tokens';

DROP TABLE json_path_values_review_map;

DROP TABLE IF EXISTS json_path_values_review_round_2;
CREATE TABLE json_path_values_review_round_2
(
    id UInt64,
    data JSON(k String, s Nullable(String)),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_review_round_2 VALUES
    (1, '{"k":"a","s":"needle"}'),
    (2, '{"k":"b","s":"other"}'),
    (3, '{"k":"c","s":null}'),
    (4, '{"k":"d"}');

SELECT toTypeName(CAST(toFixedString('a', 3), 'LowCardinality(FixedString(3))'));
SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE data.k = CAST(toFixedString('a', 3), 'LowCardinality(FixedString(3))')
SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE data.k = CAST(toFixedString('a', 3), 'LowCardinality(FixedString(3))');

SELECT toTypeName(CAST(toNullable(toFixedString('a', 3)), 'LowCardinality(Nullable(FixedString(3)))'));
SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE data.k = CAST(toNullable(toFixedString('a', 3)), 'LowCardinality(Nullable(FixedString(3)))')
SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE data.k = CAST(toNullable(toFixedString('a', 3)), 'LowCardinality(Nullable(FixedString(3)))');

SELECT count() = 0
FROM
(
    EXPLAIN indexes = 1
    SELECT count()
    FROM json_path_values_review_round_2
    WHERE data.k = CAST(toFixedString('a', 3), 'LowCardinality(FixedString(3))')
)
WHERE explain LIKE '%Name: tokens%';

SELECT id, match(data.s, 'needle') AS matched
FROM json_path_values_review_round_2
WHERE id IN (3, 4) OR matched
ORDER BY id
SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT id, match(data.s, 'needle') AS matched
FROM json_path_values_review_round_2
WHERE id IN (3, 4) OR matched
ORDER BY id;

SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE NOT match(data.s, 'needle')
SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE NOT match(data.s, 'needle');

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM json_path_values_review_round_2
    WHERE NOT match(data.s, 'needle')
)
WHERE explain LIKE '%__text_index%';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM json_path_values_review_round_2
    WHERE match(data.s, 'needle')
)
WHERE explain LIKE '%__text_index%';

DROP TABLE json_path_values_review_round_2;
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
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s = 'Alpha-needle'
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s IN ('Alpha-needle', 'beta-suffix')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE has(data.tags, 'red')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.opt = 'set'
SETTINGS force_data_skipping_indices = 'data_tokens';

SELECT 'patterns';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE startsWith(data.s, 'Alpha-')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE 'beta-%'
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE '%needle%'
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s ILIKE '%ALPHA%'
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE endsWith(data.s, '-tail')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE match(data.s, '^Alpha-.*le$')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE multiSearchAny(data.s, ['needle', 'suffix'])
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE multiSearchAnyUTF8(data.s, ['Äpf', '東京'])
SETTINGS force_data_skipping_indices = 'data_tokens';

SELECT 'path and boolean composition';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.other LIKE '%needle%'
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix PREWHERE data.s = 'Alpha-needle';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s = 'Alpha-needle' AND data.n = 10
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s = 'Alpha-needle' OR data.n = 20
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE NOT data.s = 'Alpha-needle';

SELECT 'dynamic equality';
SET dynamic_throw_on_type_mismatch = 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE dynamic_data.value = 42
SETTINGS force_data_skipping_indices = 'dynamic_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE dynamic_data.value = '42'
SETTINGS force_data_skipping_indices = 'dynamic_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE dynamic_data.value = 0.0
SETTINGS force_data_skipping_indices = 'dynamic_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE CAST(dynamic_data.value AS String) = '42';

SELECT 'ground truth and read modes';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE '%needle%'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE '%needle%'
SETTINGS query_plan_direct_read_from_text_index = 0;
SET allow_experimental_text_index_lazy_apply = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s LIKE '%needle%';

SELECT 'unsupported analyzer forms';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE data.s = ''
SETTINGS optimize_empty_string_comparisons = 0;
SELECT count() = 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_analyzer_matrix WHERE data.s = ''
    SETTINGS optimize_empty_string_comparisons = 0
)
WHERE position(explain, '__text_index') > 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE CAST(data.n AS String) = '10';
SELECT count() = 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_analyzer_matrix WHERE CAST(data.n AS String) = '10'
)
WHERE position(explain, '__text_index') > 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_analyzer_matrix WHERE dynamic_data.unsupported = 1
SETTINGS dynamic_throw_on_type_mismatch = 0, force_data_skipping_indices = 'dynamic_tokens';

SELECT 'supported plans';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_analyzer_matrix WHERE data.s = 'Alpha-needle'
    SETTINGS query_plan_optimize_count_from_text_index = 0
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
