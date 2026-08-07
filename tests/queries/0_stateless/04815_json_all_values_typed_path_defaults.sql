DROP TABLE IF EXISTS json_all_values_typed_path_defaults;

CREATE TABLE json_all_values_typed_path_defaults
(
    data JSON(x UInt16),
    INDEX bloom_idx JSONAllValues(data) TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX token_idx JSONAllValues(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_typed_path_defaults VALUES ('{}'), ('{"x":1}');

SELECT count() FROM json_all_values_typed_path_defaults
WHERE data.x = 0 SETTINGS force_data_skipping_indices = 'bloom_idx';

SELECT count() FROM json_all_values_typed_path_defaults
WHERE data.x IN (0) SETTINGS force_data_skipping_indices = 'bloom_idx';

SELECT count() FROM json_all_values_typed_path_defaults
WHERE data.x = 0 SETTINGS force_data_skipping_indices = 'token_idx';

SELECT count() FROM json_all_values_typed_path_defaults
WHERE data.x IN (0) SETTINGS force_data_skipping_indices = 'token_idx';

DROP TABLE json_all_values_typed_path_defaults;
