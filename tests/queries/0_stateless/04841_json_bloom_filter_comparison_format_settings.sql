CREATE TABLE json_bf_comparison_format_settings
(
    id UInt64,
    j JSON(flag Bool),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_comparison_format_settings FORMAT JSONEachRow
{"id":1,"j":{"flag":true}}

SELECT count()
FROM json_bf_comparison_format_settings
WHERE j.flag = 'no'
SETTINGS
    bool_true_representation = 'no',
    bool_false_representation = 'maybe',
    force_data_skipping_indices = 'idx';

DROP TABLE json_bf_comparison_format_settings;
