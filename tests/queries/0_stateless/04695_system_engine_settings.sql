-- Verify system.engine_settings table exists and has correct structure
SELECT count() > 0 FROM system.engine_settings;

-- Verify required columns exist
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'engine_settings' ORDER BY position;

-- Verify MergeTree settings are present
SELECT count() > 0 FROM system.engine_settings WHERE engine_name = 'MergeTree';

-- Verify Memory settings are present
SELECT count() > 0 FROM system.engine_settings WHERE engine_name = 'Memory';

-- Verify basic settings metadata for a known MergeTree setting
SELECT engine_name, name, changed, readonly, is_obsolete FROM system.engine_settings WHERE name = 'index_granularity' AND engine_name = 'MergeTree';

-- Verify basic settings metadata for a known Memory setting
SELECT engine_name, name, default, type FROM system.engine_settings WHERE name = 'compress' AND engine_name = 'Memory';

-- Verify multiple engines have settings
SELECT count(DISTINCT engine_name) > 3 FROM system.engine_settings;

-- Verify every listed engine accepts a SETTINGS clause
SELECT DISTINCT engine_name FROM system.engine_settings
WHERE engine_name NOT IN (SELECT name FROM system.table_engines WHERE supports_settings);

-- Verify DeltaLakeLocal lists the same settings as DeltaLake: both read the data lake settings
SELECT count() FROM
(
    (SELECT name FROM system.engine_settings WHERE engine_name = 'DeltaLake'
     EXCEPT SELECT name FROM system.engine_settings WHERE engine_name = 'DeltaLakeLocal')
    UNION ALL
    (SELECT name FROM system.engine_settings WHERE engine_name = 'DeltaLakeLocal'
     EXCEPT SELECT name FROM system.engine_settings WHERE engine_name = 'DeltaLake')
);

-- Verify settings a table takes from elsewhere unless its definition states them are listed with inherited = 1: format
-- settings, PostgreSQL's pool settings, MySQL's mysql_datatypes_support_level, NATS and RabbitMQ credentials, and the
-- settings that fall back to server settings or the server configuration when not stated. The format settings the
-- streaming engines set themselves are not inherited, see 05404_engine_settings_inherited.
SELECT countIf(NOT inherited), count() > 0 FROM system.engine_settings
WHERE (name LIKE 'format\_%' OR name LIKE 'input\_format\_%' OR name LIKE 'output\_format\_%'
    OR name IN ('postgresql_connection_pool_size', 'mysql_datatypes_support_level', 'nats_username', 'nats_password',
                'nats_token', 'nats_credential_file', 'rabbitmq_username', 'rabbitmq_password')
    OR (engine_name = 'Kafka' AND name IN ('kafka_max_block_size', 'kafka_poll_max_batch_size', 'kafka_poll_timeout_ms',
        'kafka_flush_interval_ms', 'kafka_security_protocol', 'kafka_sasl_mechanism', 'kafka_sasl_username',
        'kafka_sasl_password', 'kafka_compression_codec', 'kafka_compression_level'))
    OR (engine_name = 'FileLog' AND name IN ('max_block_size', 'poll_max_batch_size', 'poll_timeout_ms'))
    OR (engine_name = 'NATS' AND name IN ('nats_max_block_size', 'nats_flush_interval_ms'))
    OR (engine_name = 'RabbitMQ' AND name IN ('rabbitmq_vhost', 'rabbitmq_max_block_size', 'rabbitmq_flush_interval_ms'))
    OR (engine_name = 'ExecutablePool' AND name = 'max_command_execution_time')
    OR (engine_name IN ('S3Queue', 'AzureQueue') AND name IN ('keeper_path',
        'min_insert_block_size_rows_for_materialized_views', 'min_insert_block_size_bytes_for_materialized_views')))
    AND NOT (engine_name IN ('Kafka', 'FileLog', 'NATS', 'RabbitMQ')
        AND name IN ('input_format_csv_detect_header', 'input_format_tsv_detect_header', 'input_format_custom_detect_header'))
    AND NOT (engine_name IN ('Kafka', 'NATS', 'RabbitMQ')
        AND name IN ('input_format_skip_unknown_fields', 'input_format_allow_errors_ratio', 'input_format_allow_errors_num'));

-- Verify settings a new table cannot use are not listed: MaterializedPostgreSQL's database engine settings and the
-- deprecated catalog settings of the data lake engines
SELECT count() FROM system.engine_settings
WHERE (engine_name = 'MaterializedPostgreSQL' AND name IN ('materialized_postgresql_use_extended_date_and_time_types',
        'materialized_postgresql_schema_list', 'materialized_postgresql_tables_list'))
    OR name IN ('storage_catalog_type', 'storage_catalog_url', 'storage_aws_access_key_id');

-- Verify an engine keeps its own settings: Set lists only these, without the format settings its class includes
SELECT name FROM system.engine_settings WHERE engine_name = 'Set' ORDER BY name;
