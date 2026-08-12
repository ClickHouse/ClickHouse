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

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.items[].price, 10)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.items[].name, 'second')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.items[].nested.score, 8)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.items[].nullable, 5)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.items[].dynamic.:`Int64`, 99)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.nullable_items[].name, 'kept');

SELECT count()
FROM json_path_values_array_json_leaves
WHERE has(data.nullable_items[].name, 'kept')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.nullable_items[].name.:`String`, 'kept')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.nullable_items[].price.:`Int64`, 0)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(shared.nullable_items[].name.:`String`, 'shared')
SETTINGS force_data_skipping_indices = 'shared_tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.items[].price, 99)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.`items[].price`, 10)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE length(data.items[]) = 2;

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE length(data.items[]) = 2
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.items[].nullable, NULL)
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.items[].tags, ['x'])
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.items[].labels, map('k', 'v'))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.items[].t, tuple(1))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SYSTEM START MERGES json_path_values_array_json_leaves;
OPTIMIZE TABLE json_path_values_array_json_leaves FINAL;

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(data.nullable_items[].price.:`Int64`, 0)
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_array_json_leaves
WHERE has(shared.nullable_items[].name.:`String`, 'shared')
SETTINGS force_data_skipping_indices = 'shared_tokens';

DROP TABLE json_path_values_array_json_leaves;
