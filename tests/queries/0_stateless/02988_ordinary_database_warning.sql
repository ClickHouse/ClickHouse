DROP DATABASE IF EXISTS 02988_ordinary;

SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE 02988_ordinary ENGINE=Ordinary;

SELECT 'Ok.' FROM system.warnings WHERE message ILIKE '%Ordinary%' and message ILIKE '%deprecated%';

DROP DATABASE IF EXISTS 02988_ordinary;
