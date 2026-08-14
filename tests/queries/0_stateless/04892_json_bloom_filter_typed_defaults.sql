DROP TABLE IF EXISTS json_bf_typed_defaults;

CREATE TABLE json_bf_typed_defaults
(
    id UInt64,
    j JSON(x UInt16, t Tuple(y UInt16)),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_typed_defaults FORMAT JSONEachRow
{"id":1,"j":{}}
{"id":2,"j":{"x":0,"t":{"y":0}}}
{"id":3,"j":{"x":1,"t":{"y":1}}}
;

SELECT 'path equals', groupArray(id) FROM json_bf_typed_defaults WHERE j.x = 0 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'path in', groupArray(id) FROM json_bf_typed_defaults WHERE j.x IN (0) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'descendant equals', groupArray(id) FROM json_bf_typed_defaults WHERE j.t.y = 0 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'descendant in', groupArray(id) FROM json_bf_typed_defaults WHERE j.t.y IN (0) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_typed_defaults;
