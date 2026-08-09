DROP TABLE IF EXISTS json_all_values_nullable_string_cast;

CREATE TABLE json_all_values_nullable_string_cast
(
    data JSON(s Nullable(String)),
    INDEX bloom_idx JSONAllValues(data) TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX token_idx JSONAllValues(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_nullable_string_cast VALUES ('{"s":null}');

SELECT count() FROM json_all_values_nullable_string_cast
WHERE data.s::String = 'x'
SETTINGS force_data_skipping_indices = 'bloom_idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM json_all_values_nullable_string_cast
WHERE data.s::String = 'x'
SETTINGS force_data_skipping_indices = 'token_idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM json_all_values_nullable_string_cast
WHERE data.s::String = 'x'; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SELECT count() FROM json_all_values_nullable_string_cast
WHERE data.s::String = 'x'
SETTINGS cast_keep_nullable = 1, force_data_skipping_indices = 'bloom_idx';

DROP TABLE json_all_values_nullable_string_cast;
