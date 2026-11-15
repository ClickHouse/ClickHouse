-- Custom settings must remember their types - whether it's a boolean or an integer.

-- Different ways to set a boolean.
SET custom_f1 = false;
SET custom_f2 = False;
SET custom_f3 = FALSE;

SET custom_n0 = 0;
SET custom_n1 = 1;

SET custom_t1 = true;
SET custom_t2 = True;
SET custom_t3 = TRUE;

SELECT '-- Custom settings from system.settings';

SELECT name, value, type FROM system.settings WHERE startsWith(name, 'custom_') ORDER BY name;

SELECT '-- Custom settings via getSetting()';

SELECT 'custom_f1' AS name, getSetting(name) AS value, toTypeName(value);
SELECT 'custom_f2' AS name, getSetting(name) AS value, toTypeName(value);
SELECT 'custom_f3' AS name, getSetting(name) AS value, toTypeName(value);

SELECT 'custom_n0' AS name, getSetting(name) AS value, toTypeName(value);
SELECT 'custom_n1' AS name, getSetting(name) AS value, toTypeName(value);

SELECT 'custom_t1' AS name, getSetting(name) AS value, toTypeName(value);
SELECT 'custom_t2' AS name, getSetting(name) AS value, toTypeName(value);
SELECT 'custom_t3' AS name, getSetting(name) AS value, toTypeName(value);

-- Built-in settings have hardcoded types.
SELECT '-- Built-in settings';

SET async_insert = false;
SELECT name, value, type FROM system.settings WHERE name = 'async_insert';
SELECT 'async_insert' AS name, getSetting(name) AS value, toTypeName(value);

SET async_insert = False;
SELECT name, value, type FROM system.settings WHERE name = 'async_insert';
SELECT 'async_insert' AS name, getSetting(name) AS value, toTypeName(value);

SET async_insert = FALSE;
SELECT name, value, type FROM system.settings WHERE name = 'async_insert';
SELECT 'async_insert' AS name, getSetting(name) AS value, toTypeName(value);

SET async_insert = 0;
SELECT name, value, type FROM system.settings WHERE name = 'async_insert';
SELECT 'async_insert' AS name, getSetting(name) AS value, toTypeName(value);

SET async_insert = 1;
SELECT name, value, type FROM system.settings WHERE name = 'async_insert';
SELECT 'async_insert' AS name, getSetting(name) AS value, toTypeName(value);

SET async_insert = true;
SELECT name, value, type FROM system.settings WHERE name = 'async_insert';
SELECT 'async_insert' AS name, getSetting(name) AS value, toTypeName(value);

SET async_insert = True;
SELECT name, value, type FROM system.settings WHERE name = 'async_insert';
SELECT 'async_insert' AS name, getSetting(name) AS value, toTypeName(value);

SET async_insert = TRUE;
SELECT name, value, type FROM system.settings WHERE name = 'async_insert';
SELECT 'async_insert' AS name, getSetting(name) AS value, toTypeName(value);
