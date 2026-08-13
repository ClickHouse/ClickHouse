CREATE TABLE json_bf_cast_string_in_format_settings
(
    id UInt64,
    j JSON(f Float64, flag Bool),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_cast_string_in_format_settings FORMAT JSONEachRow
{"id":1,"j":{"f":1.234,"flag":true}}

SELECT count()
FROM json_bf_cast_string_in_format_settings
WHERE CAST(j.f AS String) IN ('1.23') AND j.flag = true
SETTINGS
    output_format_float_precision = 2,
    force_data_skipping_indices = 'idx';

DROP TABLE json_bf_cast_string_in_format_settings;
