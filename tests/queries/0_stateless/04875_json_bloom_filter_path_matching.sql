DROP TABLE IF EXISTS json_bf_path_matching;

CREATE TABLE json_bf_path_matching
(
    id UInt64,
    j JSON(
        a1 Array(Array(JSON)),
        a Array(JSON(f Array(Array(JSON))))
    ),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_path_matching FORMAT JSONEachRow
{"id":1,"j":{"key_1":1,"obj":{"key_7":7},"a1":[[{"x":41}]],"a":[{"f":[[{"g":42}]]}]}}
{"id":2,"j":{"key_1":2,"obj":{"key_7":257},"a1":[[{"x":-42}]],"a":[{"f":[[{"g":-43}]]}]}}
;

SELECT groupArray(id) FROM json_bf_path_matching WHERE j.key_1 = 2 SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_path_matching WHERE j.obj.key_7 = 257 SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_path_matching WHERE has(j.a1[][].x, [-42::Int64]) SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_path_matching WHERE has(j.a[].f[][].g, [[-43::Int64]]) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_path_matching;
