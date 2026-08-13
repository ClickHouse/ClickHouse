CREATE TABLE json_bf_typed_cast_in
(
    id UInt64,
    j JSON(n Int64, flag Bool),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_typed_cast_in FORMAT JSONEachRow
{"id":1,"j":{"n":257,"flag":true}}

SELECT count()
FROM json_bf_typed_cast_in
WHERE CAST(j.n AS UInt8) IN (1) AND j.flag = true
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_typed_cast_in;
