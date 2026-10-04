SELECT '-- Test against Boolean Setting';
SET optimize_on_insert;
SELECT getSetting('optimize_on_insert');
SELECT 'ok';

-- `SET name` with no value means `SET name = true`, so it only makes sense for a Bool setting.
-- The parser accepts the shorthand for any name - it does not know the settings schema - and the
-- type is checked when the change is applied.
SELECT '-- Test against String Setting';
SET default_database_engine; -- { serverError TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- Test against UInt64 Setting';
SET max_threads; -- { serverError TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- Test against Seconds Setting';
SET max_execution_time; -- { serverError TYPE_MISMATCH }
SELECT 'ok';

SELECT '-- Test with normal syntax works';
SET max_threads = 4;
SELECT getSetting('max_threads');
SELECT 'ok';

SELECT '-- A rejected shorthand leaves the setting alone';
SET max_threads; -- { serverError TYPE_MISMATCH }
SELECT getSetting('max_threads');
SELECT 'ok';

-- The client applies a query's SETTINGS to its own context as well, and rejects the shorthand
-- there, so the error can surface from either side.
SELECT '-- The same check applies to the SETTINGS clause of a query';
SELECT 1 SETTINGS optimize_on_insert;
SELECT 1 SETTINGS max_threads; -- { error TYPE_MISMATCH }
SELECT getSetting('max_threads');
SELECT 'ok';

SELECT '-- An unknown setting is still reported as unknown';
SET this_setting_does_not_exist; -- { serverError UNKNOWN_SETTING }
SELECT 'ok';
