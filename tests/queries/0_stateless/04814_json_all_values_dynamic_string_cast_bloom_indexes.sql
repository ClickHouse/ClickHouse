DROP TABLE IF EXISTS json_dynamic_string_bloom;
DROP TABLE IF EXISTS json_dynamic_string_token;
DROP TABLE IF EXISTS json_dynamic_string_ngram;
DROP TABLE IF EXISTS json_dynamic_string_sparse;

CREATE TABLE json_dynamic_string_bloom
(
    data JSON,
    INDEX idx JSONAllValues(data) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_dynamic_string_bloom VALUES
    ('{"value":2026}'),
    ('{"value":2020}'),
    ('{}');

SELECT count() FROM json_dynamic_string_bloom
WHERE data.value::String = '2026'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_bloom
WHERE data.value::String IN ('2026')
SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_dynamic_string_bloom WHERE data.value::String = '';
SELECT count() FROM json_dynamic_string_bloom
WHERE data.value::String = ''
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
WHERE data.value::String = '2026'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE data.value::String IN ('2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE data.value::String LIKE '%2026%'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE startsWith(data.value::String, '2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE hasToken(data.value::String, '2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE match(data.value::String, '^2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_token
WHERE multiSearchAny(data.value::String, ['missing', '2026'])
SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_dynamic_string_token WHERE startsWith(data.value::String, '');
SELECT count() FROM json_dynamic_string_token
WHERE startsWith(data.value::String, '')
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_dynamic_string_token WHERE match(data.value::String, '^$');
SELECT count() FROM json_dynamic_string_token
WHERE match(data.value::String, '^$')
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
WHERE data.value::String LIKE '%2026%'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_ngram
WHERE startsWith(data.value::String, '2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_ngram
WHERE endsWith(data.value::String, '2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_ngram
WHERE match(data.value::String, '^2026')
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
    ('{"value":2026}'),
    ('{"value":2020}'),
    ('{}');

SELECT count() FROM json_dynamic_string_sparse
WHERE data.value::String LIKE '%2026%'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_sparse
WHERE startsWith(data.value::String, '2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_sparse
WHERE endsWith(data.value::String, '2026')
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_dynamic_string_sparse
WHERE match(data.value::String, '^2026')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_dynamic_string_sparse;
