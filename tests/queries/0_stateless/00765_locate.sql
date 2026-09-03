SET send_logs_level = 'fatal';

SELECT '-- negative tests';
SELECT locate(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT locate(1, 'abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT locate('abc', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT locate('abc', 'abc', 'abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- test mysql compatibility setting';
SELECT locate('abcabc', 'ca');
SELECT locate('abcabc', 'ca') SETTINGS function_locate_has_mysql_compatible_argument_order = true;
SELECT locate('abcabc', 'ca') SETTINGS function_locate_has_mysql_compatible_argument_order = false;

SELECT '-- the function name needs to be case-insensitive for historical reasons';
SELECT LoCaTe('abcabc', 'ca');
