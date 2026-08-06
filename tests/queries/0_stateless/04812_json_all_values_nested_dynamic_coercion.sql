DROP TABLE IF EXISTS json_all_values_nested_dynamic;

SET input_format_try_infer_datetimes = 1;
SET input_format_try_infer_datetimes_only_datetime64 = 1;

CREATE TABLE json_all_values_nested_dynamic
(
    data JSON,
    INDEX idx JSONAllValues(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_nested_dynamic VALUES
    ('{"arr":["2026-01-01 00:00:00",1]}'),
    ('{"arr":["2020-05-05 10:00:00",2]}');

SELECT DISTINCT dynamicType(data.arr) FROM json_all_values_nested_dynamic;
SELECT count() FROM json_all_values_nested_dynamic
WHERE has(data.arr.:`Array(Dynamic)`, toDateTime64('2026-01-01 00:00:00', 9));
SELECT count() FROM json_all_values_nested_dynamic
WHERE has(data.arr.:`Array(Dynamic)`, toDateTime64('2026-01-01 00:00:00', 9))
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_all_values_nested_dynamic;
