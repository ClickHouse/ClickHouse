DROP TABLE IF EXISTS json_all_values_type_subcolumn_defaults;

CREATE TABLE json_all_values_type_subcolumn_defaults
(
    data JSON,
    INDEX bloom_idx JSONAllValues(data) TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX ngram_idx JSONAllValues(data) TYPE ngrambf_v1(2, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_type_subcolumn_defaults VALUES ('{"a":[1,2]}'), ('{"b":5}');

SELECT count() FROM json_all_values_type_subcolumn_defaults
WHERE data.a.:`Array(Nullable(Int64))` = [1, 2]
SETTINGS force_data_skipping_indices = 'bloom_idx';

SELECT count() FROM json_all_values_type_subcolumn_defaults
WHERE data.a.:`Array(Nullable(Int64))` = [];

SELECT count() FROM json_all_values_type_subcolumn_defaults
WHERE data.a.:`Array(Nullable(Int64))` IN ([]);

SELECT count() FROM json_all_values_type_subcolumn_defaults
WHERE data.a.:`Array(Nullable(Int64))`::String = '[]';

SELECT count() FROM json_all_values_type_subcolumn_defaults
WHERE data.a.:`Map(String, Nullable(Int64))`::String = '{}';

SELECT count() FROM json_all_values_type_subcolumn_defaults
WHERE data.a.:`Array(Nullable(Int64))` = []
SETTINGS ignore_data_skipping_indices = 'bloom_idx';

SELECT count() FROM json_all_values_type_subcolumn_defaults
WHERE data.a.:`Array(Nullable(Int64))` IN ([])
SETTINGS ignore_data_skipping_indices = 'bloom_idx';

SELECT count() FROM json_all_values_type_subcolumn_defaults
WHERE data.a.:`Array(Nullable(Int64))`::String LIKE '[]';

SELECT count() FROM json_all_values_type_subcolumn_defaults
WHERE data.a.:`Map(String, Nullable(Int64))`::String LIKE '{}';

DROP TABLE json_all_values_type_subcolumn_defaults;
