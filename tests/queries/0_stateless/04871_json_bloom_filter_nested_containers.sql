DROP TABLE IF EXISTS json_bf_nested_containers;

CREATE TABLE json_bf_nested_containers
(
    id UInt64,
    j JSON(arr Array(Tuple(x Int64)), flag Bool),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_nested_containers FORMAT JSONEachRow
{"id":1,"j":{"arr":[{"x":1},{"x":2}],"flag":true,"v":"007"}}
{"id":2,"j":{"arr":[{"x":3}],"flag":true,"v":"8"}}
;

SELECT 'array tuple role', groupArray(id)
FROM json_bf_nested_containers
WHERE has(j.arr.x, 2)
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'nested cast', groupArray(id)
FROM json_bf_nested_containers
WHERE CAST(CAST(j.v AS Int64) AS String) = '7' AND j.flag = true
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_nested_containers;
