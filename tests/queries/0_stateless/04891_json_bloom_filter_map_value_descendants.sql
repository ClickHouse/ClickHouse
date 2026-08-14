DROP TABLE IF EXISTS json_bf_map_value_descendants;

CREATE TABLE json_bf_map_value_descendants
(
    id UInt64,
    j JSON(
        tuple_map Map(String, Tuple(a Int64)),
        json_map Map(String, JSON)
    ),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_map_value_descendants FORMAT JSONEachRow
{"id":1,"j":{"tuple_map":{"foo":{"a":1}},"json_map":{"foo":{"a":11}}}}
{"id":2,"j":{"tuple_map":{"foo":{"a":2}},"json_map":{"foo":{"a":12}}}}
;

SET optimize_functions_to_subcolumns = 0;
SELECT 'tuple unoptimized', groupArray(id) FROM json_bf_map_value_descendants WHERE j.tuple_map['foo'].a = 2 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'json unoptimized', groupArray(id) FROM json_bf_map_value_descendants WHERE j.json_map['foo'].a = 12 SETTINGS force_data_skipping_indices = 'idx';

SET optimize_functions_to_subcolumns = 1;
SELECT 'tuple optimized', groupArray(id) FROM json_bf_map_value_descendants WHERE j.tuple_map['foo'].a = 2 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'json optimized', groupArray(id) FROM json_bf_map_value_descendants WHERE j.json_map['foo'].a = 12 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_map_value_descendants;
