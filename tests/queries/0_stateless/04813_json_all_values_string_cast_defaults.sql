DROP TABLE IF EXISTS json_all_values_string_cast_default;

CREATE TABLE json_all_values_string_cast_default
(
    data JSON(x UInt16),
    INDEX idx JSONAllValues(data) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_string_cast_default VALUES ('{}'), ('{"x":1}');

SELECT count() FROM json_all_values_string_cast_default WHERE data.x::String = '0';
SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String = '0' SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default WHERE data.x::String IN ('0');
SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String IN ('0') SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String = '1' SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String IN ('1') SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_all_values_string_cast_default;

CREATE TABLE json_all_values_string_cast_default
(
    data JSON(x UInt16),
    INDEX idx JSONAllValues(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_string_cast_default VALUES ('{}'), ('{"x":1}');

SELECT count() FROM json_all_values_string_cast_default WHERE data.x::String = '0';
SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String = '0' SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default WHERE data.x::String IN ('0');
SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String IN ('0') SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default WHERE data.x::String LIKE '%0%';
SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String LIKE '%0%' SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE startsWith(data.x::String, '0') SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE endsWith(data.x::String, '0') SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE hasToken(data.x::String, '0') SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE multiSearchAny(data.x::String, ['missing', '0']) SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String = '1' SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String IN ('1') SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String LIKE '%1%' SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String != '0' SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_all_values_string_cast_default
WHERE data.x::String NOT LIKE '%0%' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_all_values_string_cast_default;
