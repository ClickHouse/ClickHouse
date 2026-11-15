#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cp $CURDIR/data_mysql_dump/dump*.sql $USER_FILES_PATH

$CLICKHOUSE_CLIENT -q "select * from file(dump1.sql, MySQLDump, 'x Nullable(Int32), y Nullable(Int32)') order by x, y"
$CLICKHOUSE_CLIENT -q "select * from file(dump1.sql, MySQLDump, 'a Nullable(Int32), b Nullable(Int32)') order by a, b settings input_format_mysql_dump_map_column_names = 0"
$CLICKHOUSE_CLIENT -q "select * from file(dump1.sql, MySQLDump, 'y Nullable(Int32), x Nullable(Int32)') order by y, x settings input_format_mysql_dump_map_column_names = 1"
$CLICKHOUSE_CLIENT -q "select * from file(dump1.sql, MySQLDump, 'x Nullable(Int32), z String') order by x, z settings input_format_skip_unknown_fields = 0" 2>&1 | grep -F -q 'INCORRECT_DATA' && echo 'OK' || echo 'FAIL'
$CLICKHOUSE_CLIENT -q "select * from file(dump1.sql, MySQLDump, 'x Nullable(Int32), z String') order by x, z settings input_format_skip_unknown_fields = 1"

echo "dump1"

$CLICKHOUSE_CLIENT -q "desc file(dump1.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump1.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump1.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump1.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump1.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file(dump1.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump1.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3'"  2>&1 | grep -F -q 'CANNOT_EXTRACT_TABLE_STRUCTURE' && echo 'OK' || echo 'FAIL'
$CLICKHOUSE_CLIENT -q "select * from file(dump1.sql, MySQLDump, 'x Nullable(Int32)') settings input_format_mysql_dump_table_name='test 3'" 2>&1 | grep -F -q 'EMPTY_DATA_PASSED' && echo 'OK' || echo 'FAIL'

echo "dump2"

$CLICKHOUSE_CLIENT -q "desc file(dump2.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump2.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump2.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump2.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"

echo "dump3"

$CLICKHOUSE_CLIENT -q "desc file(dump3.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump3.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump3.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file(dump3.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"

echo "dump4"

$CLICKHOUSE_CLIENT -q "desc file(dump4.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump4.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump4.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file(dump4.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

echo "dump5"

$CLICKHOUSE_CLIENT -q "desc file(dump5.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump5.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump5.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump5.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"

echo "dump6"

$CLICKHOUSE_CLIENT -q "desc file(dump6.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump6.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump6.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump6.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump6.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file(dump6.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump6.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file(dump6.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

echo "dump7"

$CLICKHOUSE_CLIENT -q "desc file(dump7.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump7.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump7.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "select * from file(dump7.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "desc file(dump7.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file(dump7.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump7.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file(dump7.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

echo "dump8"

$CLICKHOUSE_CLIENT -q "desc file(dump8.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump8.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump8.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump2.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"

echo "dump9"

$CLICKHOUSE_CLIENT -q "desc file(dump9.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump9.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump9.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump9.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"

echo "dump10"

$CLICKHOUSE_CLIENT -q "desc file(dump10.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump10.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump10.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump10.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"

echo "dump11"

$CLICKHOUSE_CLIENT -q "desc file(dump11.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump11.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump11.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump11.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"


echo "dump12"

$CLICKHOUSE_CLIENT -q "desc file(dump12.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump12.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump12.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump12.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump12.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file(dump12.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump12.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file(dump12.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

echo "dump13"

$CLICKHOUSE_CLIENT -q "desc file(dump13.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump13.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump13.sql, MySQLDump) settings input_format_mysql_dump_table_name='fruits'"
$CLICKHOUSE_CLIENT -q "select * from file(dump13.sql, MySQLDump) settings input_format_mysql_dump_table_name='fruits', max_threads=1"

echo "dump14"

$CLICKHOUSE_CLIENT -q "desc file(dump14.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump14.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump14.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump14.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump14.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file(dump14.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump14.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file(dump14.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

echo "dump15"

$CLICKHOUSE_CLIENT -q "desc file(dump15.sql, MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file(dump15.sql, MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump15.sql, MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file(dump15.sql, MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump15.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file(dump15.sql, MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file(dump15.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file(dump15.sql, MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

rm $USER_FILES_PATH/dump*.sql
