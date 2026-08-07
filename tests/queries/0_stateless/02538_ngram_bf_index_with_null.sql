DROP TABLE IF EXISTS 02538_bf_ngrambf_map_values_test;

CREATE TABLE 02538_bf_ngrambf_map_values_test (`row_id` Int128, `map` Map(String, String), `map_fixed` Map(FixedString(2), String),
INDEX map_values_ngrambf mapKeys(map) TYPE ngrambf_v1(4, 256, 2, 0) GRANULARITY 1,
INDEX map_fixed_values_ngrambf mapKeys(map_fixed) TYPE ngrambf_v1(4, 256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree
ORDER BY row_id
SETTINGS index_granularity = 1;

INSERT INTO 02538_bf_ngrambf_map_values_test VALUES (1, {'a': 'a'}, {'b': 'b'});

SELECT * FROM 02538_bf_ngrambf_map_values_test PREWHERE (map['']) = 'V2V\0V2V2V2V2V2V2' WHERE (map[NULL]) = 'V2V\0V2V2V2V2V2V2V2V\0V2V2V2V2V2V2V2V\0V2V2V2V2V2V2V2V\0V2V2V2V2V2V2' SETTINGS force_data_skipping_indices = 'map_values_ngrambf';

DROP TABLE 02538_bf_ngrambf_map_values_test;
