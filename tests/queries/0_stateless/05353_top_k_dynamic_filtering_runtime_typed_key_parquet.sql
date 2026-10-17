-- Tags: no-fasttest
-- no-fasttest: Parquet

-- Same as 05353_top_k_dynamic_filtering_runtime_typed_key, for the top-K filter pushed down to a Parquet source.

SET allow_suspicious_types_in_order_by = 1;
SET use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1;
SET query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05353_0.parquet', Parquet)
SELECT toJSONString(map('a', number + 1000)) AS j FROM numbers(2000) WHERE number % 2 = 0 SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05353_1.parquet', Parquet)
SELECT toJSONString(map('a', number + 1000)) AS j FROM numbers(2000) WHERE number % 2 = 1 SETTINGS engine_file_truncate_on_insert = 1;

SELECT j FROM file(currentDatabase() || '_05353_{0,1}.parquet', Parquet, 'j JSON') ORDER BY j LIMIT 3;
