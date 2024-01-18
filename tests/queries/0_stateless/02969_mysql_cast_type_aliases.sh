#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires mysql client

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# See https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html#function_cast
# Tests are in order of the type appearance in the docs

echo "-- Uppercase tests"
#### Not supported as it is translated to FixedString without arguments
# ${MYSQL_CLIENT} --execute "SELECT 'Binary' AS type, CAST('' AS BINARY) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Binary(N)' AS type, CAST('foo' AS BINARY(3)) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Char' AS type, CAST(44 AS CHAR) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Date' AS type, CAST('2021-02-03' AS DATE) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'DateTime' AS type, CAST('2021-02-03 12:01:02' AS DATETIME) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Decimal' AS type, CAST(45.1 AS DECIMAL) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Decimal(M)' AS type, CAST(46.2 AS DECIMAL(4)) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Decimal(M, D)' AS type, CAST(47.21 AS DECIMAL(4, 2)) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Double' AS type, CAST(48.11 AS DOUBLE) AS result;"
${MYSQL_CLIENT} --execute "SET allow_experimental_object_type = 1; SELECT 'JSON' AS type, CAST('{\"foo\":\"bar\"}' AS JSON) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Real' AS type, CAST(49.22 AS REAL) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Signed' AS type, CAST(50 AS SIGNED) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Unsigned' AS type, CAST(52 AS UNSIGNED) AS result;"
#### Could be added as an alias, but SIGNED INTEGER in CAST context means UInt64, 
#### while INTEGER SIGNED as a column definition means UInt32.
# ${MYSQL_CLIENT} --execute "SELECT 'Signed integer' AS type, CAST(51 AS SIGNED INTEGER) AS result;"
# ${MYSQL_CLIENT} --execute "SELECT 'Unsigned integer' AS type, CAST(53 AS UNSIGNED INTEGER) AS result;"
${MYSQL_CLIENT} --execute "SELECT 'Year' AS type, CAST(2007 AS YEAR) AS result;"
#### Currently, expects UInt64 as an argument
# ${MYSQL_CLIENT} --execute "SELECT 'Time' AS type, CAST('12:45' AS TIME) AS result;"

echo "-- Lowercase tests"
# ${MYSQL_CLIENT} --execute "select 'Binary' as type, cast('' as binary) as result;"
${MYSQL_CLIENT} --execute "select 'Binary(N)' as type, cast('foo' as binary(3)) as result;"
${MYSQL_CLIENT} --execute "select 'Char' as type, cast(44 as char) as result;"
${MYSQL_CLIENT} --execute "select 'Date' as type, cast('2021-02-03' as date) as result;"
${MYSQL_CLIENT} --execute "select 'DateTime' as type, cast('2021-02-03 12:01:02' as datetime) as result;"
${MYSQL_CLIENT} --execute "select 'Decimal' as type, cast(45.1 as decimal) as result;"
${MYSQL_CLIENT} --execute "select 'Decimal(M)' as type, cast(46.2 as decimal(4)) as result;"
${MYSQL_CLIENT} --execute "select 'Decimal(M, D)' as type, cast(47.21 as decimal(4, 2)) as result;"
${MYSQL_CLIENT} --execute "select 'Double' as type, cast(48.11 as double) as result;"
${MYSQL_CLIENT} --execute "set allow_experimental_object_type = 1; select 'JSON' as type, cast('{\"foo\":\"bar\"}' as json) as result;"
${MYSQL_CLIENT} --execute "select 'Real' as type, cast(49.22 as real) as result;"
${MYSQL_CLIENT} --execute "select 'Signed' as type, cast(50 as signed) as result;"
${MYSQL_CLIENT} --execute "select 'Unsigned' as type, cast(52 as unsigned) as result;"
# ${MYSQL_CLIENT} --execute "select 'Signed integer' as type, cast(51 as signed integer) as result;"
# ${MYSQL_CLIENT} --execute "select 'Unsigned integer' as type, cast(53 as unsigned integer) as result;"
${MYSQL_CLIENT} --execute "select 'Year' as type, cast(2007 as year) as result;"
# ${MYSQL_CLIENT} --execute "select 'Time' as type, cast('12:45' as time) as result;"
