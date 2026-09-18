SET allow_experimental_full_text_index = 1;
SET enable_analyzer = 1;

CREATE TABLE t_json_all_values_variant_cast
(
    data JSON(v Variant(UInt64, String)),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_all_values_variant_cast VALUES ('{"v":42}');

-- An identity `Variant` cast must preserve the numeric alternative's type error.
SELECT count() FROM t_json_all_values_variant_cast
WHERE match(CAST(data.v AS Variant(UInt64, String)), 'zzz')
SETTINGS use_skip_indexes = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM t_json_all_values_variant_cast
WHERE match(CAST(data.v AS Variant(UInt64, String)), 'zzz')
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count() FROM t_json_all_values_variant_cast
WHERE match(CAST(data.v AS Variant(UInt64, String)), 'zzz')
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Casting to `String` still has the indexed representation and remains accelerated.
SELECT count() FROM t_json_all_values_variant_cast
WHERE match(CAST(data.v AS String), 'zzz')
SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_values';

DROP TABLE t_json_all_values_variant_cast;
