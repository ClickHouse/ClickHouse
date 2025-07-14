-- See https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html#function_cast
-- Tests are in order of the type appearance in the docs

SET allow_experimental_json_type = 1;

SELECT '-- Uppercase tests';
-- Not supported as it is translated to FixedString without arguments
-- SELECT 'Binary' AS mysql_type, CAST('' AS BINARY) AS result, toTypeName(result) AS native_type;
SELECT 'Binary(N)' AS mysql_type, CAST('foo' AS BINARY(3)) AS result, toTypeName(result) AS native_type;
SELECT 'Char' AS mysql_type, CAST(44 AS CHAR) AS result, toTypeName(result) AS native_type;
SELECT 'Date' AS mysql_type, CAST('2021-02-03' AS DATE) AS result, toTypeName(result) AS native_type;
SELECT 'DateTime' AS mysql_type, CAST('2021-02-03 12:01:02' AS DATETIME) AS result, toTypeName(result) AS native_type;
SELECT 'Decimal' AS mysql_type, CAST(45.1 AS DECIMAL) AS result, toTypeName(result) AS native_type;
SELECT 'Decimal(M)' AS mysql_type, CAST(46.2 AS DECIMAL(4)) AS result, toTypeName(result) AS native_type;
SELECT 'Decimal(M, D)' AS mysql_type, CAST(47.21 AS DECIMAL(4, 2)) AS result, toTypeName(result) AS native_type;
SELECT 'Double' AS mysql_type, CAST(48.11 AS DOUBLE) AS result, toTypeName(result) AS native_type;
SELECT 'JSON' AS mysql_type, CAST('{\"foo\":\"bar\"}' AS JSON) AS result, toTypeName(result) AS native_type;
SELECT 'Real' AS mysql_type, CAST(49.22 AS REAL) AS result, toTypeName(result) AS native_type;
SELECT 'Signed' AS mysql_type, CAST(50 AS SIGNED) AS result, toTypeName(result) AS native_type;
SELECT 'Unsigned' AS mysql_type, CAST(52 AS UNSIGNED) AS result, toTypeName(result) AS native_type;
-- Could be added as an alias, but SIGNED INTEGER in CAST context means UInt64, 
-- while INTEGER SIGNED as a column definition means UInt32.
-- SELECT 'Signed integer' AS mysql_type, CAST(51 AS SIGNED INTEGER) AS result, toTypeName(result) AS native_type;
-- SELECT 'Unsigned integer' AS mysql_type, CAST(53 AS UNSIGNED INTEGER) AS result, toTypeName(result) AS native_type;
SELECT 'Year' AS mysql_type, CAST(2007 AS YEAR) AS result, toTypeName(result) AS native_type;
-- Currently, expects UInt64 as an argument
-- SELECT 'Time' AS mysql_type, CAST('12:45' AS TIME) AS result, toTypeName(result) AS native_type;

SELECT '-- Lowercase tests';
-- select 'Binary' as mysql_type, cast('' as binary) as result, toTypeName(result) as native_type;
select 'Binary(N)' as mysql_type, cast('foo' as binary(3)) as result, toTypeName(result) as native_type;
select 'Char' as mysql_type, cast(44 as char) as result, toTypeName(result) as native_type;
select 'Date' as mysql_type, cast('2021-02-03' as date) as result, toTypeName(result) as native_type;
select 'DateTime' as mysql_type, cast('2021-02-03 12:01:02' as datetime) as result, toTypeName(result) as native_type;
select 'Decimal' as mysql_type, cast(45.1 as decimal) as result, toTypeName(result) as native_type;
select 'Decimal(M)' as mysql_type, cast(46.2 as decimal(4)) as result, toTypeName(result) as native_type;
select 'Decimal(M, D)' as mysql_type, cast(47.21 as decimal(4, 2)) as result, toTypeName(result) as native_type;
select 'Double' as mysql_type, cast(48.11 as double) as result, toTypeName(result) as native_type;
select 'JSON' as mysql_type, cast('{\"foo\":\"bar\"}' as json) as result, toTypeName(result) as native_type;
select 'Real' as mysql_type, cast(49.22 as real) as result, toTypeName(result) as native_type;
select 'Signed' as mysql_type, cast(50 as signed) as result, toTypeName(result) as native_type;
select 'Unsigned' as mysql_type, cast(52 as unsigned) as result, toTypeName(result) as native_type;
-- select 'Signed integer' as mysql_type, cast(51 as signed integer) as result, toTypeName(result) as native_type;
-- select 'Unsigned integer' as mysql_type, cast(53 as unsigned integer) as result, toTypeName(result) as native_type;
select 'Year' as mysql_type, cast(2007 as year) as result, toTypeName(result) as native_type;
-- select 'Time' as mysql_type, cast('12:45' as time) as result, toTypeName(result) as native_type;
