DROP DATABASE IF EXISTS 02988_ordinary;

SET allow_deprecated_database_ordinary = 1;
SET send_logs_level='fatal';
-- Creation of a database with Ordinary engine emits a warning.
SET send_logs_level='fatal';
CREATE DATABASE 02988_ordinary ENGINE=Ordinary;
SET send_logs_level='warning';

SELECT 'Ok.' FROM system.warnings WHERE message ILIKE '%Ordinary%' and message ILIKE '%deprecated%';

DROP DATABASE IF EXISTS 02988_ordinary;
