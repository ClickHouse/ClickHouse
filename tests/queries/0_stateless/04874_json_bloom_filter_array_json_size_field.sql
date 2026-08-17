DROP TABLE IF EXISTS json_bf_array_json_size_field;

CREATE TABLE json_bf_array_json_size_field
(
    id UInt64,
    j JSON(arr Array(JSON)),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_array_json_size_field FORMAT JSONEachRow
{"id":1,"j":{"arr":[{"inner":{"size0":91}}]}}
{"id":2,"j":{"arr":[{"inner":{"size0":-92}}]}}
;

SELECT groupArray(id)
FROM json_bf_array_json_size_field
WHERE has(j.arr[].inner.size0, -92::Int64)
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_array_json_size_field;
