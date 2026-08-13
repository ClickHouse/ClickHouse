DROP TABLE IF EXISTS json_bf_typed_descendants;

CREATE TABLE json_bf_typed_descendants
(
    id UInt64,
    j JSON(payload JSON),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_typed_descendants FORMAT JSONEachRow
{"id":1,"j":{"payload":{"items":11}}}
{"id":2,"j":{"payload":{"items":22}}}
;

SELECT groupArray(id)
FROM json_bf_typed_descendants
WHERE j.payload.items = 22
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_typed_descendants;
