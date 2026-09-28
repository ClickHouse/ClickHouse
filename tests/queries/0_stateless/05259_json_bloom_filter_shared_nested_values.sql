-- `jsonbf_v1` over complex values in the shared data of JSON: arrays of objects, arrays of arrays, arrays with NULLs
-- and nested objects. With `max_dynamic_paths = 0` all of them are kept in shared data.

SET allow_experimental_json_bloom_filter_index = 1;

DROP TABLE IF EXISTS json_bf_shared_nested;

CREATE TABLE json_bf_shared_nested
(
    id UInt64,
    j JSON(max_dynamic_paths = 0),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES json_bf_shared_nested;

INSERT INTO json_bf_shared_nested FORMAT JSONEachRow
{"id":1,"j":{"arr":[{"x":1,"s":"a"},{"x":2,"n":null}],"obj":{"k":"v1","inner":{"z":10}},"strs":["p",null,"q"],"nested":[[{"y":5}]]}}
{"id":2,"j":{"arr":[{"x":3,"s":"b","deep":[{"w":"d2"}]}],"obj":{"k":"v2","inner":{"z":20}},"strs":["r"],"nested":[[{"y":6}],[{"y":7}]]}}
;

INSERT INTO json_bf_shared_nested FORMAT JSONEachRow
{"id":3,"j":{"arr":[],"obj":{"k":"v3","inner":null},"other":42}}
{"id":4,"j":{"arr":[{"x":4,"s":"c","deep":[{"w":"d4"}]},{"x":5,"deep":[{"w":null}]}],"strs":[null],"nested":[[],[{"y":8,"t":"u"}]]}}
;

SELECT 'shared', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'json_bf_shared_nested' AND active AND column = 'j' AND arrayExists(s -> s LIKE 'j.object_shared_data%', substreams);

SELECT 'check parts';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE has(j.arr[].x, 2::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE has(j.arr[].s, 'c') SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE j.obj.k = 'v2' SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE j.obj.inner.z = 20 SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE has(j.nested[][].y, [7::Int64]) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE has(j.arr[].deep[].w, ['d4']) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE j.other = 42 SETTINGS force_data_skipping_indices = 'idx';

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM json_bf_shared_nested WHERE j.obj.inner.z = 20)
WHERE explain LIKE '%Granules%' ORDER BY explain LIMIT 1 SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

SYSTEM START MERGES json_bf_shared_nested;
OPTIMIZE TABLE json_bf_shared_nested FINAL;

SELECT 'check merged part';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE has(j.arr[].x, 2::Int64) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE has(j.arr[].s, 'c') SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE j.obj.k = 'v2' SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE j.obj.inner.z = 20 SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE has(j.nested[][].y, [7::Int64]) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE has(j.arr[].deep[].w, ['d4']) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_shared_nested WHERE j.other = 42 SETTINGS force_data_skipping_indices = 'idx';

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM json_bf_shared_nested WHERE j.obj.inner.z = 20)
WHERE explain LIKE '%Granules%' ORDER BY explain LIMIT 1 SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

DROP TABLE json_bf_shared_nested;
