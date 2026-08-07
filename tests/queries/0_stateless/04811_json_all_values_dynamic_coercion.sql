DROP TABLE IF EXISTS json_all_values_dynamic_bloom;
DROP TABLE IF EXISTS json_all_values_dynamic_token;

SET input_format_try_infer_datetimes = 1;
SET input_format_try_infer_datetimes_only_datetime64 = 1;
SET session_timezone = 'UTC';

CREATE TABLE json_all_values_dynamic_bloom
(
    data JSON,
    INDEX idx JSONAllValues(data) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_dynamic_bloom VALUES
    ('{"ts":"2026-01-01 00:00:00"}'),
    ('{"ts":"2020-05-05 10:00:00"}');

SELECT DISTINCT dynamicType(data.ts) FROM json_all_values_dynamic_bloom;
SELECT count() FROM json_all_values_dynamic_bloom
WHERE data.ts = toDate('2026-01-01') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

CREATE TABLE json_all_values_dynamic_token
(
    data JSON,
    INDEX idx JSONAllValues(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_dynamic_token
SELECT * FROM json_all_values_dynamic_bloom;

SELECT count() FROM json_all_values_dynamic_token
WHERE data.ts = toDate('2026-01-01') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_all_values_dynamic_bloom;
DROP TABLE json_all_values_dynamic_token;
