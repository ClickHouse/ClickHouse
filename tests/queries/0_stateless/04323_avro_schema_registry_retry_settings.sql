-- Tags: no-fasttest
-- Validate the bounds on `format_avro_schema_registry_max_retries` and
-- `format_avro_schema_registry_retry_initial_backoff_ms`. The format
-- factory rejects out-of-range values up front so misconfiguration
-- surfaces immediately instead of on the first registry call.

-- max_retries cap is 20, anything higher must be rejected.
DESC format(AvroConfluent, '') SETTINGS format_avro_schema_registry_url = 'http://invalid:1', format_avro_schema_registry_max_retries = 21; -- { clientError BAD_ARGUMENTS }

-- initial_backoff must be > 0.
DESC format(AvroConfluent, '') SETTINGS format_avro_schema_registry_url = 'http://invalid:1', format_avro_schema_registry_retry_initial_backoff_ms = 0; -- { clientError BAD_ARGUMENTS }

-- initial_backoff cap is 60000 ms.
DESC format(AvroConfluent, '') SETTINGS format_avro_schema_registry_url = 'http://invalid:1', format_avro_schema_registry_retry_initial_backoff_ms = 60001; -- { clientError BAD_ARGUMENTS }

-- Boundary values that the validator must accept: max_retries = 0 (disables
-- retries) and initial_backoff = 60000 (the upper bound). The query still
-- fails on the empty-input magic-byte check.
DESC format(AvroConfluent, '') SETTINGS format_avro_schema_registry_url = 'http://invalid:1', format_avro_schema_registry_max_retries = 0, format_avro_schema_registry_retry_initial_backoff_ms = 60000; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
