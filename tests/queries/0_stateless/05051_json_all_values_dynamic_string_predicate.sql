SET allow_experimental_full_text_index = 1;
SET input_format_try_infer_datetimes = 1;
SET input_format_try_infer_datetimes_only_datetime64 = 0;

DROP TABLE IF EXISTS json_all_values_dynamic_string_predicate;

CREATE TABLE json_all_values_dynamic_string_predicate
(
    data JSON(max_dynamic_paths = 1),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO json_all_values_dynamic_string_predicate
SELECT '{"dynamic_dt":"2040-03-03 00:00:00"}';

SELECT count()
FROM json_all_values_dynamic_string_predicate
WHERE startsWith(data.dynamic_dt, 'zzz'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE json_all_values_dynamic_string_predicate;
