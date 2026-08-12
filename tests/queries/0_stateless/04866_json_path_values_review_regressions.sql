SET enable_json_type = 1;
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
