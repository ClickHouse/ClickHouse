-- Tags: no-fasttest
-- Bounds on the Confluent Schema Registry settings. `0` is rejected by their `NonZeroUInt64` type.
-- Too-high values are clamped, never rejected: reading them runs for every query, so a rejection
-- would fail every statement after the `SET` that introduced it, including the `SET` undoing it.
SET send_logs_level = 'fatal'; -- the clamp warns, and the warning would fail the test on stderr

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

-- Boundary values that must be accepted unchanged: `max_retries = 0` disables retries. Every
-- setting raised above its maximum above is brought back in bounds here, because the client keeps
-- an unclamped copy of them - see the note on the `SETTINGS` clause below.
SET format_avro_schema_registry_max_retries = 0;
SET format_avro_schema_registry_retry_initial_backoff_ms = 60000;
SET format_avro_schema_registry_connection_timeout = 599;
SET format_avro_schema_registry_send_timeout = 599;
SET format_avro_schema_registry_receive_timeout = 599;
SELECT name, value FROM system.settings WHERE name LIKE 'format_avro_schema_registry_%timeout'
    OR name IN ('format_avro_schema_registry_max_retries', 'format_avro_schema_registry_retry_initial_backoff_ms')
    ORDER BY name;

-- The registry is still only contacted when a query actually needs it. A `SETTINGS` clause makes
-- clickhouse-client clamp its own settings copy, which logs to its stderr and is not covered by
-- `send_logs_level`, so no setting may be left above its maximum by the time this runs.
DESC format(AvroConfluent, '') SETTINGS format_avro_schema_registry_url = 'http://invalid:1'; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
