DROP TABLE IF EXISTS json_bf_in_null;

CREATE TABLE json_bf_in_null
(
    id UInt8,
    j JSON(x UInt8),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_in_null VALUES (1, '{"x":1}'), (2, '{"x":2}');

SELECT groupArray(id)
FROM json_bf_in_null
WHERE j.x IN (NULL, 1)
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_in_null;
