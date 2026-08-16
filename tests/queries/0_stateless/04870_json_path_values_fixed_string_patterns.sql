SET enable_json_type = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_like_min_pattern_length = 1;

DROP TABLE IF EXISTS json_pv_fixed_string_patterns;
CREATE TABLE json_pv_fixed_string_patterns
(
    id UInt64,
    json JSON(k FixedString(3)),
    INDEX idx json TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_pv_fixed_string_patterns;
INSERT INTO json_pv_fixed_string_patterns VALUES
    (1, '{"k":"a"}'),
    (2, '{"k":"ab"}'),
    (3, '{"k":"b"}');

SELECT arraySort(groupArray(id)) FROM json_pv_fixed_string_patterns
WHERE startsWith(json.k, 'a')
SETTINGS force_data_skipping_indices = 'idx';

SELECT arraySort(groupArray(id)) FROM json_pv_fixed_string_patterns
WHERE endsWith(json.k, 'a')
SETTINGS force_data_skipping_indices = 'idx';

SELECT arraySort(groupArray(id)) FROM json_pv_fixed_string_patterns
WHERE json.k LIKE 'a%'
SETTINGS force_data_skipping_indices = 'idx';

SELECT arraySort(groupArray(id)) FROM json_pv_fixed_string_patterns
WHERE json.k ILIKE 'a%'
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_pv_fixed_string_patterns;
