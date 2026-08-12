SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_index_tokens_arrays;
CREATE TABLE json_index_tokens_arrays
(
    id UInt64,
    typed JSON(
        tags Array(String),
        numbers Array(Int64),
        other Array(String),
        nullable_tags Array(Nullable(String)),
        floats Array(Float64)),
    shared JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    dynamic JSON(max_dynamic_paths = 16, max_dynamic_types = 16),
    INDEX typed_tokens typed TYPE text(tokenizer = jsonPathValues(64)),
    INDEX shared_tokens shared TYPE text(tokenizer = jsonPathValues(64)),
    INDEX dynamic_tokens dynamic TYPE text(tokenizer = jsonPathValues(64))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_index_tokens_arrays;
INSERT INTO json_index_tokens_arrays VALUES
    (1,
     '{"tags":["foo",""],"numbers":[1,2],"other":["bar"],"nullable_tags":[null,"ᴺᵁᴸᴸ"],"floats":[-0.0,1.5]}',
     '{"tags":["foo",""],"nullable_tags":[null,"ᴺᵁᴸᴸ"],"floats":[-0.0,1.5]}',
     '{"tags":["foo",""],"nullable_tags":[null,"ᴺᵁᴸᴸ"],"floats":[-0.0,1.5]}'),
    (2,
     concat('{"tags":["bar","', repeat('x', 100), '"],"numbers":[2,3],"nullable_tags":["other"],"floats":[0.0,2.5]}'),
     concat('{"tags":["bar","', repeat('x', 100), '"],"nullable_tags":["other"],"floats":[0.0,2.5]}'),
     concat('{"tags":["bar","', repeat('x', 100), '"],"nullable_tags":["other"],"floats":[0.0,2.5]}'));
INSERT INTO json_index_tokens_arrays VALUES
    (3, '{"tags":[],"numbers":[]}', '{"tags":[]}', '{"tags":[]}'),
    (4, '{}', '{}', '{}');

SELECT 'typed arrays';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.tags, 'foo')
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.tags, '');
SELECT count() FROM json_index_tokens_arrays WHERE has(typed.tags, '')
SETTINGS force_data_skipping_indices = 'typed_tokens'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.numbers, 2)
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.tags, repeat('x', 100))
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.tags, 'bar')
SETTINGS force_data_skipping_indices = 'typed_tokens';

SELECT 'nullable and float arrays';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.nullable_tags, 'ᴺᵁᴸᴸ')
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.floats, toFloat64(0.0))
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.floats, toFloat64(-0.0))
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.nullable_tags, CAST(NULL, 'Nullable(String)'));
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_arrays WHERE has(typed.nullable_tags, CAST(NULL, 'Nullable(String)'))
)
WHERE position(explain, '__text_index') > 0;

SELECT 'path isolation and array equality';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.tags, 'bar') AND has(typed.other, 'bar')
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE typed.tags = ['foo', ''];
SELECT count() FROM json_index_tokens_arrays WHERE typed.tags = ['foo', '']
SETTINGS force_data_skipping_indices = 'typed_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'shared Dynamic arrays';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(shared.tags.:`Array(Nullable(String))`, 'foo');
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(shared.tags.:`Array(Nullable(String))`, '');
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(shared.tags.:`Array(Nullable(String))`, repeat('x', 100));
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(shared.nullable_tags.:`Array(Nullable(String))`, 'ᴺᵁᴸᴸ');
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(shared.floats.:`Array(Nullable(Float64))`, toFloat64(0.0));
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE shared.tags.:`Array(Nullable(String))` = ['foo', ''];
SELECT count() FROM json_index_tokens_arrays WHERE shared.tags.:`Array(Nullable(String))` = ['foo', '']
SETTINGS force_data_skipping_indices = 'shared_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'Dynamic path arrays';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(dynamic.tags.:`Array(Nullable(String))`, 'foo');
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(dynamic.tags.:`Array(Nullable(String))`, '');
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(dynamic.tags.:`Array(Nullable(String))`, repeat('x', 100));
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(dynamic.nullable_tags.:`Array(Nullable(String))`, 'ᴺᵁᴸᴸ');
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(dynamic.floats.:`Array(Nullable(Float64))`, toFloat64(-0.0));
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE dynamic.tags.:`Array(Nullable(String))` = ['foo', ''];
SELECT count() FROM json_index_tokens_arrays WHERE dynamic.tags.:`Array(Nullable(String))` = ['foo', '']
SETTINGS force_data_skipping_indices = 'dynamic_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'direct read plan';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_arrays WHERE has(typed.tags, 'foo')
)
WHERE position(explain, '__text_index') > 0;

SYSTEM START MERGES json_index_tokens_arrays;
OPTIMIZE TABLE json_index_tokens_arrays FINAL;

SELECT 'merged arrays';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(typed.tags, 'foo')
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(shared.nullable_tags.:`Array(Nullable(String))`, 'ᴺᵁᴸᴸ');
SELECT groupArray(id) FROM json_index_tokens_arrays WHERE has(dynamic.floats.:`Array(Nullable(Float64))`, toFloat64(0.0));

DROP TABLE json_index_tokens_arrays;
