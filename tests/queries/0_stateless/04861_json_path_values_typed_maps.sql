SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;
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
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE mapContains(data.attrs, 'dotted.key')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE mapContainsKey(data.attrs, 'quoted"key')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE has(data.attrs, concat('zero', char(0), 'key'))
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE has(data.attrs, 'empty')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE has(data.attrs, '')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'keyed equality';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE data.attrs['normal'] = 'value'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE data.attrs[''] = 'empty-key'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE data.attrs['dotted.key'] = 'dot'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE data.attrs['quoted"key'] = 'quote'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE data.attrs[concat('zero', char(0), 'key')] = 'zero'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE data.attrs['truncated'] = repeat('x', 100)
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE data.attrs['dup'] = 'first'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE data.attrs['dup'] = 'second'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE data.lc['low'] = 'card'
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
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE has(data.attrs, repeat('k', 300));
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
SELECT groupArray(id) FROM json_path_values_typed_maps WHERE data.attrs['normal'] = 'value'
SETTINGS force_data_skipping_indices = 'tokens';

DROP TABLE json_path_values_typed_maps;
