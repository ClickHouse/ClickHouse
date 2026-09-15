DROP TABLE IF EXISTS json_bf_map_descendant_skip_paths;

CREATE TABLE json_bf_map_descendant_skip_paths
(
    id UInt64,
    j JSON(
        tuple_map Map(String, Tuple(a Int64, b Int64)),
        json_map Map(String, JSON)
    ),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1,
    INDEX idx_exact j TYPE jsonbf_v1(
        false_positive_rate = 0.0001,
        skip_paths = ['tuple_map.a', 'json_map.a']) GRANULARITY 1,
    INDEX idx_regexp j TYPE jsonbf_v1(
        false_positive_rate = 0.0001,
        skip_paths_regexp = ['\\.a$']) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_map_descendant_skip_paths FORMAT JSONEachRow
{"id":1,"j":{"tuple_map":{"foo":{"a":1,"b":11}},"json_map":{"foo":{"a":21,"b":31}}}}
{"id":2,"j":{"tuple_map":{"foo":{"a":2,"b":12}},"json_map":{"foo":{"a":22,"b":32}}}}
;

SELECT 'tuple unoptimized', groupArray(id) FROM json_bf_map_descendant_skip_paths WHERE j.tuple_map['foo'].a = 2
SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0;
SELECT 'json unoptimized', groupArray(id) FROM json_bf_map_descendant_skip_paths WHERE j.json_map['foo'].a = 22
SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0;
SELECT 'tuple optimized', groupArray(id) FROM json_bf_map_descendant_skip_paths WHERE j.tuple_map['foo'].a = 2
SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1;
SELECT 'json optimized', groupArray(id) FROM json_bf_map_descendant_skip_paths WHERE j.json_map['foo'].a = 22
SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1;

SELECT 'tuple exact kept', groupArray(id) FROM json_bf_map_descendant_skip_paths WHERE j.tuple_map['foo'].b = 12
SETTINGS force_data_skipping_indices = 'idx_exact';
SELECT 'json exact kept', groupArray(id) FROM json_bf_map_descendant_skip_paths WHERE j.json_map['foo'].b = 32
SETTINGS force_data_skipping_indices = 'idx_exact';
SELECT 'tuple regexp kept', groupArray(id) FROM json_bf_map_descendant_skip_paths WHERE j.tuple_map['foo'].b = 12
SETTINGS force_data_skipping_indices = 'idx_regexp';
SELECT 'json regexp kept', groupArray(id) FROM json_bf_map_descendant_skip_paths WHERE j.json_map['foo'].b = 32
SETTINGS force_data_skipping_indices = 'idx_regexp';

SELECT count() FROM json_bf_map_descendant_skip_paths WHERE j.tuple_map['foo'].a = 2
SETTINGS force_data_skipping_indices = 'idx_exact'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_bf_map_descendant_skip_paths WHERE j.json_map['foo'].a = 22
SETTINGS force_data_skipping_indices = 'idx_exact'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_bf_map_descendant_skip_paths WHERE j.tuple_map['foo'].a = 2
SETTINGS force_data_skipping_indices = 'idx_regexp'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_bf_map_descendant_skip_paths WHERE j.json_map['foo'].a = 22
SETTINGS force_data_skipping_indices = 'idx_regexp'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_bf_map_descendant_skip_paths;
