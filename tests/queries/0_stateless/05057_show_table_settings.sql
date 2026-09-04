-- The statement is a thin rewrite over `system.table_settings`. These check that it parses, that it
-- is told apart from the two statements it shares a prefix with, and that its filters reach through.

DROP TABLE IF EXISTS mt;
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = 12345;

SELECT '-- only what something other than the default set';
SHOW CHANGED TABLE SETTINGS FROM mt;

SELECT '-- one setting, by name';
SHOW TABLE SETTINGS FROM mt LIKE 'min_bytes_for_wide_part';

SELECT '-- ILIKE is case-insensitive';
SHOW CHANGED TABLE SETTINGS FROM mt ILIKE 'MIN_BYTES%';

SELECT '-- NOT inverts it';
SHOW CHANGED TABLE SETTINGS FROM mt NOT LIKE 'min_bytes%';

SELECT '-- IN is accepted in place of FROM';
SHOW CHANGED TABLE SETTINGS IN mt LIKE 'min_bytes%';

SELECT '-- a database-qualified name parses and resolves';
-- `system.one` has no settings, so this returns nothing; what it checks is that the qualified form
-- reaches the right table rather than being read as a bare name.
SHOW TABLE SETTINGS FROM system.one;

SELECT '-- the statements sharing its prefix still parse as themselves';
SHOW TABLES;
SHOW SETTINGS LIKE 'add_http_cors_header';

DROP TABLE mt;
