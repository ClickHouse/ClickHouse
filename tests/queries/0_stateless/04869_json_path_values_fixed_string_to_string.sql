SET enable_json_type = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_pv_fixed_string_to_string;
CREATE TABLE json_pv_fixed_string_to_string
(
    id UInt64,
    json JSON(k FixedString(3), s String),
    INDEX idx json TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_pv_fixed_string_to_string;
INSERT INTO json_pv_fixed_string_to_string VALUES
    (1, '{"k":"a","s":"a"}'),
    (2, '{"k":"b","s":"b"}');

-- `toString` removes `FixedString` padding, so it is not transparent to the index.
SELECT count() FROM json_pv_fixed_string_to_string WHERE toString(json.k) = unhex('610000');
SELECT count() FROM json_pv_fixed_string_to_string WHERE toString(json.k) = unhex('610000')
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM json_pv_fixed_string_to_string WHERE toString(json.k) IN ('a');
SELECT count() FROM json_pv_fixed_string_to_string WHERE toString(json.k) IN ('a')
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM json_pv_fixed_string_to_string WHERE has(['a'], toString(json.k));
SELECT count() FROM json_pv_fixed_string_to_string WHERE has(['a'], toString(json.k))
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- Plain `String` remains transparent.
SELECT count() FROM json_pv_fixed_string_to_string WHERE toString(json.s) IN ('a')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_pv_fixed_string_to_string;
