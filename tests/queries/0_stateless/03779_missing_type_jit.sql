SET compile_expressions = 1, min_count_to_compile_expression = 0, allow_suspicious_low_cardinality_types = 1;

CREATE TABLE date_t__fuzz_45 (`id` Nullable(Time64(3)), `value1` Array(Array(UUID)), `date1` LowCardinality(Nullable(Date32))) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS allow_nullable_key = 1;

SELECT value1 FROM date_t__fuzz_45 PREWHERE (toStartOfSecond(date1) < 199203) AND (199300 < 1) WHERE (199203 <= toStartOfSecond(date1)) AND (1 < toLowCardinality(199300)) QUALIFY isNull(id) SETTINGS enable_analyzer = 1;
