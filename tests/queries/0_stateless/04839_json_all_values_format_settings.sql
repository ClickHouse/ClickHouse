DROP TABLE IF EXISTS json_all_values_format_settings;

CREATE TABLE json_all_values_format_settings
(
    data JSON(b Bool, arr Array(DateTime('UTC')), u UInt64),
    INDEX bf JSONAllValues(data) TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX tbf JSONAllValues(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_format_settings VALUES
    ('{"b":true,"arr":["2024-06-01 12:00:00"],"u":42,"d":true}'),
    ('{"b":false,"arr":["2020-01-01 00:00:00"],"u":7,"d":false}');

SELECT 'typed equals', count()
FROM json_all_values_format_settings
WHERE data.b = 'no'
SETTINGS bool_true_representation = 'no', bool_false_representation = 'maybe', force_data_skipping_indices = 'bf,tbf';

SELECT 'typed in', count()
FROM json_all_values_format_settings
WHERE data.b IN ('no')
SETTINGS bool_true_representation = 'no', bool_false_representation = 'maybe', force_data_skipping_indices = 'bf,tbf';

SELECT 'typed string cast', count()
FROM json_all_values_format_settings
WHERE data.b::String = 'yes'
SETTINGS bool_true_representation = 'yes', bool_false_representation = 'maybe';

SELECT 'dynamic string cast', count()
FROM json_all_values_format_settings
WHERE data.d::String = 'yes'
SETTINGS bool_true_representation = 'yes', bool_false_representation = 'maybe';

SELECT 'datetime string cast', count()
FROM json_all_values_format_settings
WHERE data.arr::String = toString([toDateTime('2024-06-01 12:00:00', 'UTC')])
SETTINGS date_time_output_format = 'unix_timestamp';

SELECT 'safe string cast', count()
FROM json_all_values_format_settings
WHERE data.u::String = '42'
SETTINGS bool_true_representation = 'yes', bool_false_representation = 'maybe', force_data_skipping_indices = 'bf,tbf';

DROP TABLE json_all_values_format_settings;
