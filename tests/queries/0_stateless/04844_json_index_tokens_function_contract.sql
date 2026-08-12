SET enable_json_type = 1;

DROP TABLE IF EXISTS json_path_values_contract;
CREATE TABLE json_path_values_contract
(
    id UInt64,
    data JSON(s String, i Int64, a Array(Nullable(String)), max_dynamic_paths = 8),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(32)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_contract VALUES
    (1, '{"s":"value","i":-7,"a":["x",null,""]}'),
    (2, concat('{"s":"', repeat('x', 200), '","dynamic":42}'));

SELECT groupArray(id) FROM json_path_values_contract WHERE data.s = 'value'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_contract WHERE has(data.a, NULL);
SELECT count() FROM json_path_values_contract WHERE has(data.a, NULL)
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_path_values_contract WHERE data.dynamic = 42
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0;
CHECK TABLE json_path_values_contract SETTINGS check_query_single_value_result = 1;

DROP TABLE json_path_values_contract;
