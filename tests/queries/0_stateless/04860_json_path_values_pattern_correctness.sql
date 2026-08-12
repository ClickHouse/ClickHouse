SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS json_path_values_pattern_correctness;
CREATE TABLE json_path_values_pattern_correctness
(
    id UInt64,
    data JSON(
        value String,
        like_pattern Nullable(String),
        ilike_pattern Nullable(String),
        start_needle Nullable(String),
        end_needle Nullable(String),
        regexp Nullable(String)),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_pattern_correctness VALUES
    (1, '{"value":"abc-value","like_pattern":"a%","ilike_pattern":"a%","start_needle":"abc","end_needle":"def","regexp":"^a"}'),
    (2, '{"value":"other","like_pattern":null,"ilike_pattern":null,"start_needle":null,"end_needle":null,"regexp":null}');

SELECT 'right-side wrappers';
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE 'abc' LIKE ifNull(data.like_pattern, '');
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE 'ABC' ILIKE ifNull(data.ilike_pattern, '');
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE startsWith('abcdef', ifNull(data.start_needle, 'zzz'));
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE endsWith('abcdef', ifNull(data.end_needle, 'zzz'));
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE match('abc', ifNull(data.regexp, '^z'));

SELECT 'commutative equality';
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE 'abc-value' = ifNull(data.value, '')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'dictionary scan setting';
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE startsWith(data.value, 'abc')
SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE startsWith(data.value, 'abc')
SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0,
    text_index_like_min_pattern_length = 3,
    force_data_skipping_indices = 'tokens';
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE data.value LIKE '%value%'
SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0,
    force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'minimum pattern length';
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE startsWith(data.value, 'ab')
SETTINGS text_index_like_min_pattern_length = 3,
    force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE endsWith(data.value, 'ue')
SETTINGS text_index_like_min_pattern_length = 3,
    force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE data.value LIKE 'ab%'
SETTINGS text_index_like_min_pattern_length = 3,
    force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_path_values_pattern_correctness
WHERE startsWith(data.value, 'abc')
SETTINGS text_index_like_min_pattern_length = 3,
    force_data_skipping_indices = 'tokens';

DROP TABLE json_path_values_pattern_correctness;
