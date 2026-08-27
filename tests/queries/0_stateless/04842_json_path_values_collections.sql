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
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.tags, 'foo')
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.tags, '');
SELECT count() FROM json_index_tokens_arrays WHERE has(typed.tags, '')
SETTINGS force_data_skipping_indices = 'typed_tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.numbers, 2)
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.tags, repeat('x', 100))
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.tags, 'bar')
SETTINGS force_data_skipping_indices = 'typed_tokens';

SELECT 'nullable and float arrays';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.nullable_tags, 'ᴺᵁᴸᴸ')
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.floats, toFloat64(0.0))
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.floats, toFloat64(-0.0))
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.nullable_tags, CAST(NULL, 'Nullable(String)'));
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_arrays WHERE has(typed.nullable_tags, CAST(NULL, 'Nullable(String)'))
)
WHERE position(explain, '__text_index') > 0;

SELECT 'path isolation and array equality';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.tags, 'bar') AND has(typed.other, 'bar')
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE typed.tags = ['foo', ''];
SELECT count() FROM json_index_tokens_arrays WHERE typed.tags = ['foo', '']
SETTINGS force_data_skipping_indices = 'typed_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'shared Dynamic arrays';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(shared.tags.:`Array(Nullable(String))`, 'foo');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(shared.tags.:`Array(Nullable(String))`, '');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(shared.tags.:`Array(Nullable(String))`, repeat('x', 100));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(shared.nullable_tags.:`Array(Nullable(String))`, 'ᴺᵁᴸᴸ');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(shared.floats.:`Array(Nullable(Float64))`, toFloat64(0.0));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE shared.tags.:`Array(Nullable(String))` = ['foo', ''];
SELECT count() FROM json_index_tokens_arrays WHERE shared.tags.:`Array(Nullable(String))` = ['foo', '']
SETTINGS force_data_skipping_indices = 'shared_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'Dynamic path arrays';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(dynamic.tags.:`Array(Nullable(String))`, 'foo');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(dynamic.tags.:`Array(Nullable(String))`, '');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(dynamic.tags.:`Array(Nullable(String))`, repeat('x', 100));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(dynamic.nullable_tags.:`Array(Nullable(String))`, 'ᴺᵁᴸᴸ');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(dynamic.floats.:`Array(Nullable(Float64))`, toFloat64(-0.0));
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE dynamic.tags.:`Array(Nullable(String))` = ['foo', ''];
SELECT count() FROM json_index_tokens_arrays WHERE dynamic.tags.:`Array(Nullable(String))` = ['foo', '']
SETTINGS force_data_skipping_indices = 'dynamic_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'direct read plan';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_arrays WHERE has(typed.tags, 'foo')
    SETTINGS query_plan_optimize_count_from_text_index = 0
)
WHERE position(explain, '__text_index') > 0;

SYSTEM START MERGES json_index_tokens_arrays;
OPTIMIZE TABLE json_index_tokens_arrays FINAL;

SELECT 'merged arrays';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(typed.tags, 'foo')
SETTINGS force_data_skipping_indices = 'typed_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(shared.nullable_tags.:`Array(Nullable(String))`, 'ᴺᵁᴸᴸ');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_arrays WHERE has(dynamic.floats.:`Array(Nullable(Float64))`, toFloat64(0.0));

DROP TABLE json_index_tokens_arrays;

DROP TABLE IF EXISTS json_index_tokens_in;
CREATE TABLE json_index_tokens_in
(
    id UInt64,
    data JSON(lc LowCardinality(String), s String),
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_index_tokens_in VALUES
    (1, '{"lc":"alpha","s":"alpha"}'),
    (2, '{"lc":"beta","s":"beta"}'),
    (3, '{"lc":"gamma","s":"gamma"}'),
    (4, '{"lc":"beta","s":"beta"}');

SELECT arraySort(groupArray(id)) FROM json_index_tokens_in WHERE data.lc IN ('alpha', 'gamma')
SETTINGS force_data_skipping_indices = 'json_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_in WHERE data.lc IN ['beta', 'gamma']
SETTINGS force_data_skipping_indices = 'json_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_in WHERE data.lc IN ('missing', 'alpha')
SETTINGS force_data_skipping_indices = 'json_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_in WHERE data.lc IN ('missing')
SETTINGS force_data_skipping_indices = 'json_tokens';

SELECT arraySort(groupArray(id)) FROM json_index_tokens_in WHERE data.lc IN ('alpha', 'gamma')
SETTINGS query_plan_direct_read_from_text_index = 0, force_data_skipping_indices = 'json_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_in WHERE data.s IN ('alpha', 'gamma')
SETTINGS force_data_skipping_indices = 'json_tokens';

SELECT count() > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_index_tokens_in WHERE data.lc IN ('alpha', 'gamma')
    SETTINGS force_data_skipping_indices = 'json_tokens'
)
WHERE explain LIKE '%mode: Any%';

SELECT any(toTypeName(data.lc IN ('alpha', 'gamma'))), arraySort(groupArray(id))
FROM json_index_tokens_in
WHERE data.lc IN ('alpha', 'gamma')
SETTINGS force_data_skipping_indices = 'json_tokens';

DROP TABLE json_index_tokens_in;

DROP TABLE IF EXISTS json_index_tokens_nullable_in;
CREATE TABLE json_index_tokens_nullable_in
(
    id UInt64,
    data JSON(s Nullable(String), fixed Nullable(FixedString(3))),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_index_tokens_nullable_in;
INSERT INTO json_index_tokens_nullable_in VALUES
    (1, '{"s":"alpha","fixed":"one"}'),
    (2, '{"s":"beta","fixed":"two"}'),
    (3, '{"s":"gamma","fixed":"one"}'),
    (4, '{"s":null,"fixed":null}'),
    (5, '{}');

SET transform_null_in = 1;

SELECT arraySort(groupArray(id)) FROM json_index_tokens_nullable_in WHERE data.s IN ('alpha', 'gamma')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_nullable_in WHERE data.s GLOBAL IN ('beta')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_nullable_in WHERE data.fixed IN ('one')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_nullable_in WHERE (data.s, id) IN (('alpha', 1), ('gamma', 3))
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id)) FROM json_index_tokens_nullable_in WHERE data.s IN ('alpha', NULL);
SELECT count() = 0
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_index_tokens_nullable_in WHERE data.s IN ('alpha', NULL)
)
WHERE explain LIKE '%Name: tokens%';

SET transform_null_in = 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_nullable_in WHERE data.s IN ('alpha', 'gamma')
SETTINGS force_data_skipping_indices = 'tokens';

DROP TABLE json_index_tokens_nullable_in;
DROP TABLE IF EXISTS json_path_values_array_json_leaves;

CREATE TABLE json_path_values_array_json_leaves
(
    id UInt64,
    data JSON(
        max_dynamic_paths = 0,
        `items[].price` Array(Int64),
        items Array(JSON(
            max_dynamic_paths = 0,
            name String,
            nested JSON(max_dynamic_paths = 0, score UInt64),
            nullable Nullable(Int64),
            price Int64,
            labels Map(String, String),
            tags Array(String),
            t Tuple(x Int64))),
        nullable_items Array(Nullable(JSON(
            max_dynamic_paths = 0,
            name String,
            price Int64)))),
    shared JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 100000000,
    INDEX shared_tokens shared TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_path_values_array_json_leaves;
INSERT INTO json_path_values_array_json_leaves VALUES
    (1, '{"items":[{"dynamic":99,"labels":{"k":"v"},"name":"first","nested":{"score":7},"nullable":null,"price":10,"tags":["x"],"t":{"x":1}},{"dynamic":"99","name":"second","nested":{"score":8},"nullable":5,"price":20}],"items[].price":[99],"nullable_items":[null,{"name":"kept","price":5}]}', '{"nullable_items":[null,{"name":"shared","price":6}]}'),
    (2, '{"items":[{"dynamic":100,"name":"other","nested":{"score":9},"nullable":6,"price":30}],"items[].price":[40],"nullable_items":[]}', '{"nullable_items":[]}'),
    (3, '{"items":[],"items[].price":[],"nullable_items":[]}', '{"nullable_items":[]}');
INSERT INTO json_path_values_array_json_leaves VALUES
    (4, '{"items":[],"items[].price":[],"nullable_items":[{"name":"zero","price":0}]}', '{"nullable_items":[]}');

SELECT count()
FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_array_json_leaves', 'tokens')
WHERE startsWith(hex(token), '6974656D730000');

SELECT count() > 0
FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_array_json_leaves', 'tokens')
WHERE startsWith(hex(token), '6974656D735B5D2E70726963650000');

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.items[].price, 10)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.items[].name, 'second')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.items[].nested.score, 8)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.items[].nullable, 5)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.items[].dynamic.:`Int64`, 99)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.nullable_items[].name, 'kept');

SELECT count()
FROM json_path_values_array_json_leaves
WHERE has(data.nullable_items[].name, 'kept')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.nullable_items[].name.:`String`, 'kept')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.nullable_items[].price.:`Int64`, 0)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(shared.nullable_items[].name.:`String`, 'shared')
SETTINGS force_data_skipping_indices = 'shared_tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.items[].price, 99)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.`items[].price`, 10)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE length(data.items[]) = 2;

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE length(data.items[]) = 2
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.items[].nullable, NULL)
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.items[].tags, ['x'])
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.items[].labels, map('k', 'v'))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.items[].t, tuple(1))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SYSTEM START MERGES json_path_values_array_json_leaves;
OPTIMIZE TABLE json_path_values_array_json_leaves FINAL;

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(data.nullable_items[].price.:`Int64`, 0)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_array_json_leaves
WHERE has(shared.nullable_items[].name.:`String`, 'shared')
SETTINGS force_data_skipping_indices = 'shared_tokens';

DROP TABLE json_path_values_array_json_leaves;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS json_path_values_typed_maps;
CREATE TABLE json_path_values_typed_maps
(
    id UInt64,
    lookup_key String,
    data JSON(
        attrs Map(String, String),
        lc Map(LowCardinality(String), LowCardinality(String)),
        unsupported Map(String, UInt64),
        max_dynamic_paths = 0),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64), dictionary_block_size = 2) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_path_values_typed_maps;
INSERT INTO json_path_values_typed_maps VALUES
(
    1,
    'normal',
    concat(
        '{"attrs":{"normal":"value","":"empty-key","empty":"","dotted.key":"dot","quoted\\"key":"quote",',
        '"zero\\u0000key":"zero","dup":"first","dup":"second","truncated":"', repeat('x', 100), '","',
        repeat('k', 300), '":"long-key"},"lc":{"low":"card"},"unsupported":{"number":1}}')
);
INSERT INTO json_path_values_typed_maps VALUES
    (2, 'other', '{"attrs":{"normal":"other","truncated":"short"},"lc":{"other":"value"}}'),
    (3, 'normal', '{"attrs":{},"lc":{}}');

SELECT 'key existence';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE has(data.attrs, 'normal')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE mapContains(data.attrs, 'dotted.key')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE mapContainsKey(data.attrs, 'quoted"key')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE has(data.attrs, concat('zero', char(0), 'key'))
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE has(data.attrs, 'empty')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE has(data.attrs, '')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'keyed equality';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.attrs['normal'] = 'value'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.attrs[''] = 'empty-key'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.attrs['dotted.key'] = 'dot'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.attrs['quoted"key'] = 'quote'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.attrs[concat('zero', char(0), 'key')] = 'zero'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.attrs['truncated'] = repeat('x', 100)
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.attrs['dup'] = 'first'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.attrs['dup'] = 'second'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.lc['low'] = 'card'
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'direct read';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM json_path_values_typed_maps WHERE data.attrs['normal'] = 'value'
)
WHERE position(explain, '__text_index') > 0;
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM json_path_values_typed_maps WHERE has(data.attrs, 'normal')
)
WHERE position(explain, '__text_index') > 0;

SELECT 'planner rejection';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.attrs['missing'] = '';
SELECT count() FROM json_path_values_typed_maps WHERE data.attrs['missing'] = ''
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_typed_maps WHERE has(data.attrs, repeat('k', 300))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE has(data.attrs, repeat('k', 300));
SELECT count() FROM json_path_values_typed_maps WHERE data.attrs['normal'] IN ('value', 'other')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_typed_maps WHERE data.attrs['normal'] LIKE 'val%'
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_typed_maps WHERE data.attrs = map('normal', 'value')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_typed_maps WHERE has(data.unsupported, 'number')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_typed_maps WHERE has(data.attrs, lookup_key)
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_typed_maps WHERE mapContainsValue(data.attrs, 'value')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_typed_maps WHERE mapExists((key, value) -> key = 'normal' AND value = 'value', data.attrs)
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SYSTEM START MERGES json_path_values_typed_maps;
OPTIMIZE TABLE json_path_values_typed_maps FINAL;

SELECT 'merged';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE has(data.attrs, 'normal')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_path_values_typed_maps WHERE data.attrs['normal'] = 'value'
SETTINGS force_data_skipping_indices = 'tokens';

DROP TABLE json_path_values_typed_maps;
