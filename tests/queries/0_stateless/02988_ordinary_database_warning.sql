DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

SET send_logs_level = 'fatal';
SET allow_deprecated_database_ordinary = 1;
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE=Ordinary;

SELECT DISTINCT 'Ok.' FROM system.warnings WHERE message ILIKE '%Ordinary%' and message ILIKE '%deprecated%';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
