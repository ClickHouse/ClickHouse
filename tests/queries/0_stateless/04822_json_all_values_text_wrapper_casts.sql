SET allow_experimental_full_text_index = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_json_all_values_wrapper_casts;

CREATE TABLE t_json_all_values_wrapper_casts
(
    data JSON(
        ip IPv4,
        tag String,
        nullable_ip Nullable(IPv4),
        nullable_tag Nullable(String)),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_json_all_values_wrapper_casts
SELECT multiIf(
    number < 4, '{"ip":"1.2.3.4","tag":"safe-needle","nullable_ip":"1.2.3.4","nullable_tag":"nullable-needle"}',
    number < 8, '{"ip":"8.8.8.8","tag":"other","nullable_ip":"8.8.8.8","nullable_tag":"other"}',
    '{}')
FROM numbers(12);

-- Adding `Nullable` and changing only `LowCardinality` preserve the indexed representation.
SELECT count() FROM t_json_all_values_wrapper_casts WHERE data.ip::Nullable(IPv4) = toIPv4('1.2.3.4');
SELECT count() FROM t_json_all_values_wrapper_casts WHERE data.tag::LowCardinality(String) = 'safe-needle';

SELECT count() FROM t_json_all_values_wrapper_casts
WHERE data.ip::Nullable(IPv4) = toIPv4('1.2.3.4')
SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_json_all_values_wrapper_casts
WHERE data.tag::LowCardinality(String) = 'safe-needle'
SETTINGS use_skip_indexes = 0;

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_json_all_values_wrapper_casts WHERE data.ip::Nullable(IPv4) = toIPv4('1.2.3.4')
)
WHERE explain LIKE '%Granules: 1/3%';

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_json_all_values_wrapper_casts WHERE data.tag::LowCardinality(String) = 'safe-needle'
)
WHERE explain LIKE '%Granules: 1/3%';

-- Removing `Nullable` must not let the index suppress an exception from NULL or missing values.
SELECT count() FROM t_json_all_values_wrapper_casts
WHERE data.nullable_ip::IPv4 = toIPv4('1.2.3.4'); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SELECT count() FROM t_json_all_values_wrapper_casts
WHERE data.nullable_ip::IPv4 = toIPv4('1.2.3.4')
SETTINGS use_skip_indexes = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SELECT count() FROM t_json_all_values_wrapper_casts
WHERE data.nullable_tag::String = 'nullable-needle'; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SELECT count() FROM t_json_all_values_wrapper_casts
WHERE data.nullable_tag::String = 'nullable-needle'
SETTINGS use_skip_indexes = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

DROP TABLE t_json_all_values_wrapper_casts;
