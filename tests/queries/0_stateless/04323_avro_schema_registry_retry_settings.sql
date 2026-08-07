-- Tags: no-fasttest
-- Bounds on the Confluent Schema Registry settings. They are applied when the value is set -
-- `0` is rejected by the settings' `NonZeroUInt64` type, values above the maximum are clamped by
-- `doSettingsSanityCheckClamp`. They must not be checked where the settings are read, in
-- `getFormatSettings`: that runs for every query, so an out-of-range value would be accepted by
-- `SET` and would then fail every following statement, including the `SET` putting it back.
-- The clamp logs a warning, which would otherwise reach the client's stderr and fail the test.
SET send_logs_level = 'fatal';

-- Zero is rejected up front, and the session survives it.
SET format_avro_schema_registry_retry_initial_backoff_ms = 0; -- { serverError BAD_ARGUMENTS }
SET format_avro_schema_registry_connection_timeout = 0; -- { serverError BAD_ARGUMENTS }
SET format_avro_schema_registry_send_timeout = 0; -- { serverError BAD_ARGUMENTS }
SET format_avro_schema_registry_receive_timeout = 0; -- { serverError BAD_ARGUMENTS }
SELECT 'session alive after zero';

-- Values above the maximum are clamped, and the session survives those too.
SET format_avro_schema_registry_max_retries = 21;
SET format_avro_schema_registry_retry_initial_backoff_ms = 60001;
SET format_avro_schema_registry_connection_timeout = 600;
SET format_avro_schema_registry_send_timeout = 600;
SET format_avro_schema_registry_receive_timeout = 600;
SELECT name, value FROM system.settings WHERE name LIKE 'format_avro_schema_registry_%timeout'
    OR name IN ('format_avro_schema_registry_max_retries', 'format_avro_schema_registry_retry_initial_backoff_ms')
    ORDER BY name;
SELECT 'session alive after too high';

-- Boundary values that must be accepted unchanged: `max_retries = 0` disables retries.
SET format_avro_schema_registry_max_retries = 0;
SET format_avro_schema_registry_retry_initial_backoff_ms = 60000;
SET format_avro_schema_registry_connection_timeout = 599;
SELECT name, value FROM system.settings WHERE name IN (
    'format_avro_schema_registry_max_retries',
    'format_avro_schema_registry_retry_initial_backoff_ms',
    'format_avro_schema_registry_connection_timeout') ORDER BY name;

-- The registry is still only contacted when a query actually needs it.
DESC format(AvroConfluent, '') SETTINGS format_avro_schema_registry_url = 'http://invalid:1'; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
