DROP TABLE IF EXISTS json_dynamic_string_bloom;
DROP TABLE IF EXISTS json_dynamic_string_token;
DROP TABLE IF EXISTS json_dynamic_string_ngram;
DROP TABLE IF EXISTS json_dynamic_string_sparse;

SET input_format_try_infer_datetimes = 1;
SET input_format_try_infer_datetimes_only_datetime64 = 1;
SET session_timezone = 'UTC';

CREATE TABLE json_dynamic_string_bloom
(
    data JSON,
    INDEX idx JSONAllValues(data) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_dynamic_string_bloom VALUES
    ('{"ts":"2026-01-01 00:00:00"}'),
    ('{"ts":"2020-05-05 10:00:00"}'),
    ('{}');

SELECT count() FROM json_dynamic_string_bloom WHERE data.ts = toDate('2026-01-01');
SELECT count() FROM json_dynamic_string_bloom
WHERE data.ts::String = '2026-01-01 00:00:00.000000000'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_bloom
WHERE data.ts::String IN ('2026-01-01 00:00:00.000000000')
SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_dynamic_string_bloom WHERE data.ts::String = '';
SELECT count() FROM json_dynamic_string_bloom
WHERE data.ts::String = ''
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

CREATE TABLE json_dynamic_string_token
(
    data JSON,
    INDEX idx JSONAllValues(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_dynamic_string_token SELECT * FROM json_dynamic_string_bloom;

SELECT count() FROM json_dynamic_string_token
WHERE data.ts::String = '2026-01-01 00:00:00.000000000'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE data.ts::String IN ('2026-01-01 00:00:00.000000000')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE data.ts::String LIKE '%2026%'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE startsWith(data.ts::String, '2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE hasToken(data.ts::String, '2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE match(data.ts::String, '^2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE multiSearchAny(data.ts::String, ['missing', '2026'])
SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_dynamic_string_token WHERE startsWith(data.ts::String, '');
SELECT count() FROM json_dynamic_string_token
WHERE startsWith(data.ts::String, '')
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_dynamic_string_token WHERE match(data.ts::String, '^$');
SELECT count() FROM json_dynamic_string_token
WHERE match(data.ts::String, '^$')
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

CREATE TABLE json_dynamic_string_ngram
(
    data JSON,
    INDEX idx JSONAllValues(data) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_dynamic_string_ngram SELECT * FROM json_dynamic_string_bloom;

SELECT count() FROM json_dynamic_string_ngram
WHERE data.ts::String LIKE '%2026%'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_ngram
WHERE startsWith(data.ts::String, '2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_ngram
WHERE endsWith(data.ts::String, '000000000')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_ngram
WHERE match(data.ts::String, '^2026')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_dynamic_string_bloom;
DROP TABLE json_dynamic_string_token;
DROP TABLE json_dynamic_string_ngram;

CREATE TABLE json_dynamic_string_sparse
(
    data JSON,
    INDEX idx JSONAllValues(data) TYPE sparse_grams(3, 100, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_dynamic_string_sparse VALUES
    ('{"ts":"2026-01-01 00:00:00"}'),
    ('{"ts":"2020-05-05 10:00:00"}'),
    ('{}');

SELECT count() FROM json_dynamic_string_sparse
WHERE data.ts::String LIKE '%2026%'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_sparse
WHERE startsWith(data.ts::String, '2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_sparse
WHERE endsWith(data.ts::String, '000000000')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_sparse
WHERE match(data.ts::String, '^2026')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_dynamic_string_sparse;
