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

-- Verify settings a table takes from elsewhere unless its definition states them are not listed: format settings,
-- PostgreSQL's pool settings, MySQL's mysql_datatypes_support_level, NATS and RabbitMQ credentials
SELECT count() FROM system.engine_settings
WHERE name LIKE 'format\_%' OR name LIKE 'input\_format\_%' OR name LIKE 'output\_format\_%'
    OR name IN ('postgresql_connection_pool_size', 'mysql_datatypes_support_level', 'nats_username', 'nats_password',
                'nats_token', 'nats_credential_file', 'rabbitmq_username', 'rabbitmq_password');

-- Verify an engine keeps its own settings: Set lists only these, without the format settings its class includes
SELECT name FROM system.engine_settings WHERE engine_name = 'Set' ORDER BY name;
