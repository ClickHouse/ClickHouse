SELECT '-- Test against Boolean Setting';
SET optimize_on_insert;
SELECT getSetting('optimize_on_insert');
SELECT 'ok';

SELECT '-- Test against String Setting';
SET default_database_engine; -- { clientError SYNTAX_ERROR }
SELECT 'ok';

SELECT '-- Test against UInt64 Setting';
SET max_threads; -- { clientError SYNTAX_ERROR }
SELECT 'ok';

SELECT '-- Test against Seconds Setting';
SET max_execution_time; -- { clientError SYNTAX_ERROR }
SELECT 'ok';

SELECT '-- Test with normal syntax works';
SET max_threads = 4;
SELECT getSetting('max_threads');
SELECT 'ok';
