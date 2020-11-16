SET custom_a = 5;
SET custom_b = -177;
SET custom_c = 98.11;
SET custom_d = 'abc def';
SELECT getSetting('custom_a') as v, toTypeName(v);
SELECT getSetting('custom_b') as v, toTypeName(v);
SELECT getSetting('custom_c') as v, toTypeName(v);
SELECT getSetting('custom_d') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name LIKE 'custom_%' ORDER BY name;

SELECT '';
SET custom_a = 'changed';
SET custom_b = NULL;
SET custom_c = 50000;
SET custom_d = 1.11;
SELECT getSetting('custom_a') as v, toTypeName(v);
SELECT getSetting('custom_b') as v, toTypeName(v);
SELECT getSetting('custom_c') as v, toTypeName(v);
SELECT getSetting('custom_d') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name LIKE 'custom_%' ORDER BY name;

SELECT '';
SELECT getSetting('custom_e') as v, toTypeName(v); -- { serverError 115 } -- Setting not found.
SET custom_e = 404;
SELECT getSetting('custom_e') as v, toTypeName(v);

SET invalid_custom = 8; -- { serverError 115 } -- Setting is neither a builtin nor started with one of the registered prefixes for user-defined settings.

SELECT '';
SELECT getSetting('custom_e') as v, toTypeName(v) SETTINGS custom_e = -0.333;
SELECT name, value FROM system.settings WHERE name = 'custom_e' SETTINGS custom_e = -0.333;
SELECT getSetting('custom_e') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name = 'custom_e';

SELECT getSetting('custom_f') as v, toTypeName(v) SETTINGS custom_f = 'word';
SELECT name, value FROM system.settings WHERE name = 'custom_f' SETTINGS custom_f = 'word';
SELECT getSetting('custom_f') as v, toTypeName(v); -- { serverError 115 } -- Setting not found.
SELECT COUNT() FROM system.settings WHERE name = 'custom_f';

SELECT '';
SET custom_compound.identifier.v1 = 'test';
SELECT getSetting('custom_compound.identifier.v1') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name = 'custom_compound.identifier.v1';

CREATE SETTINGS PROFILE s1_01418 SETTINGS custom_compound.identifier.v2 = 100;
SHOW CREATE SETTINGS PROFILE s1_01418;
DROP SETTINGS PROFILE s1_01418;
