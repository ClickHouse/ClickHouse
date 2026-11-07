-- Tags: no-random-settings
-- no-random-settings: the test asserts the defaults a `compatibility` pin produces, and an
-- explicitly set setting is not affected by the pin.

-- A `compatibility` pin must reproduce the defaults the pinned release shipped: an entry recorded in
-- the wrong version in the setting's declaration reverts a default the pinned release had, or
-- keeps one it did not have.

SELECT 'shipped in 26.8';
SET compatibility = '26.8';
SELECT name, value FROM system.settings
WHERE name IN ('query_plan_optimize_lazy_materialization_for_object_storage',
               'distributed_cache_min_inflight_bytes_to_discard_connection_on_seek',
               'distributed_plan_workers_provisioning_timeout_ms')
ORDER BY name;

SELECT 'not yet in 26.7';
SET compatibility = '26.7';
SELECT name, value FROM system.settings
WHERE name IN ('query_plan_optimize_lazy_materialization_for_object_storage',
               'distributed_cache_min_inflight_bytes_to_discard_connection_on_seek',
               'distributed_plan_workers_provisioning_timeout_ms')
ORDER BY name;

SELECT 'also shipped in 26.8';
SET compatibility = '26.8';
SELECT value FROM system.settings WHERE name = 'query_plan_aggregation_bucket_top_k';
SET compatibility = '26.7';
SELECT value FROM system.settings WHERE name = 'query_plan_aggregation_bucket_top_k';

SELECT 'enabled in 26.1';
SET compatibility = '26.1';
SELECT value FROM system.settings WHERE name = 'use_skip_indexes_on_data_read';
SET compatibility = '25.12';
SELECT value FROM system.settings WHERE name = 'use_skip_indexes_on_data_read';

SELECT 'backported to 26.8';
SET compatibility = '26.8';
SELECT name, value FROM system.settings
WHERE name IN ('ai_function_max_api_calls_per_query', 'ai_function_max_concurrent_requests_per_thread',
               'ai_function_max_retries', 'backup_s3_expired_token_retry_timeout_ms',
               'iceberg_tolerate_conflicting_manifest_schemas')
ORDER BY name;
SET compatibility = '26.7';
SELECT name, value FROM system.settings
WHERE name IN ('ai_function_max_api_calls_per_query', 'ai_function_max_concurrent_requests_per_thread',
               'ai_function_max_retries', 'backup_s3_expired_token_retry_timeout_ms',
               'iceberg_tolerate_conflicting_manifest_schemas')
ORDER BY name;

SELECT 'backported to 26.7';
SET compatibility = '26.7';
SELECT name, value FROM system.settings
WHERE name IN ('ai_function_max_input_tokens_per_query', 'ai_function_max_output_tokens_per_query',
               'azure_validate_etag_on_read')
ORDER BY name;
SET compatibility = '26.6';
SELECT name, value FROM system.settings
WHERE name IN ('ai_function_max_input_tokens_per_query', 'ai_function_max_output_tokens_per_query',
               'azure_validate_etag_on_read')
ORDER BY name;

SELECT 'backported to 26.9, or to 26.8 but not to 26.9';
SET compatibility = '26.9';
SELECT name, value FROM system.settings
WHERE name IN ('azure_validate_etag_on_read', 'use_text_index_postings_cache')
ORDER BY name;
SET compatibility = '26.8';
SELECT name, value FROM system.settings
WHERE name IN ('azure_validate_etag_on_read', 'use_text_index_postings_cache')
ORDER BY name;

SELECT 'backported to 26.1';
SET compatibility = '26.1';
SELECT name, value FROM system.settings
WHERE name IN ('http_headers_read_timeout', 'http_max_field_name_size', 'http_max_fields', 'http_max_request_header_size')
ORDER BY name;
SET compatibility = '25.12';
SELECT name, value FROM system.settings
WHERE name IN ('http_headers_read_timeout', 'http_max_field_name_size', 'http_max_fields', 'http_max_request_header_size')
ORDER BY name;

SELECT 'backported to 26.3 and 26.5, not to 26.4';
SET compatibility = '26.5';
SELECT value FROM system.settings WHERE name = 'filesystem_cache_wait_for_concurrent_download_timeout_milliseconds';
SET compatibility = '26.4';
SELECT value FROM system.settings WHERE name = 'filesystem_cache_wait_for_concurrent_download_timeout_milliseconds';
SET compatibility = '26.3';
SELECT value FROM system.settings WHERE name = 'filesystem_cache_wait_for_concurrent_download_timeout_milliseconds';
SET compatibility = '26.2';
SELECT value FROM system.settings WHERE name = 'filesystem_cache_wait_for_concurrent_download_timeout_milliseconds';
