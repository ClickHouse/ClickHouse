DROP TABLE IF EXISTS json_bf_nested_containers;

CREATE TABLE json_bf_nested_containers
(
    id UInt64,
    j JSON(
        arr Array(Tuple(x Int64)),
        flag Bool,
        payload JSON,
        array_json Array(JSON),
        a1 Array(Array(JSON)),
        a Array(JSON(f Array(Array(JSON))))),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_nested_containers FORMAT JSONEachRow
{"id":1,"j":{"arr":[{"x":1},{"x":2}],"flag":true,"v":"007","payload":{"items":11},"array_json":[{"inner":{"size0":91}}],"key_1":1,"obj":{"key_7":7},"a1":[[{"x":41}]],"a":[{"f":[[{"g":42}]]}]}}
{"id":2,"j":{"arr":[{"x":3}],"flag":true,"v":"8","payload":{"items":22},"array_json":[{"inner":{"size0":-92}}],"key_1":2,"obj":{"key_7":257},"a1":[[{"x":-42}]],"a":[{"f":[[{"g":-43}]]}]}}
;

SELECT 'array tuple role', groupArray(id) FROM json_bf_nested_containers WHERE has(j.arr.x, 2)
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'nested cast', groupArray(id) FROM json_bf_nested_containers WHERE CAST(CAST(j.v AS Int64) AS String) = '7'
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id) FROM json_bf_nested_containers WHERE j.payload.items = 22
SETTINGS force_data_skipping_indices = 'idx';

SELECT groupArray(id) FROM json_bf_nested_containers WHERE has(j.array_json[].inner.size0, -92::Int64)
SETTINGS force_data_skipping_indices = 'idx';

SELECT groupArray(id) FROM json_bf_nested_containers WHERE j.key_1 = 2 SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_nested_containers WHERE j.obj.key_7 = 257 SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_nested_containers WHERE has(j.a1[][].x, [-42::Int64]) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_nested_containers WHERE has(j.a[].f[][].g, [[-43::Int64]]) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_nested_containers;
