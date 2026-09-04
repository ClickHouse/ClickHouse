#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cp $CURDIR/data_mysql_dump/dump*.sql $CLICKHOUSE_USER_FILES_UNIQUE

$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump, 'x Nullable(Int32), y Nullable(Int32)') order by x, y"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump, 'a Nullable(Int32), b Nullable(Int32)') order by a, b settings input_format_mysql_dump_map_column_names = 0"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump, 'y Nullable(Int32), x Nullable(Int32)') order by y, x settings input_format_mysql_dump_map_column_names = 1"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump, 'x Nullable(Int32), z String') order by x, z settings input_format_skip_unknown_fields = 0" 2>&1 | grep -F -q 'INCORRECT_DATA' && echo 'OK' || echo 'FAIL'
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump, 'x Nullable(Int32), z String') order by x, z settings input_format_skip_unknown_fields = 1"

echo "dump1"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3'"  2>&1 | grep -F -q 'CANNOT_EXTRACT_TABLE_STRUCTURE' && echo 'OK' || echo 'FAIL'
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump1.sql', MySQLDump, 'x Nullable(Int32)') settings input_format_mysql_dump_table_name='test 3'" 2>&1 | grep -F -q 'EMPTY_DATA_PASSED' && echo 'OK' || echo 'FAIL'

echo "dump2"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump2.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump2.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump2.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump2.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"

echo "dump3"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump3.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump3.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump3.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump3.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"

echo "dump4"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump4.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump4.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump4.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump4.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

echo "dump5"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump5.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump5.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump5.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump5.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"

echo "dump6"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump6.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump6.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump6.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump6.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump6.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump6.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump6.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump6.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

echo "dump7"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump7.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump7.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump7.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump7.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump7.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump7.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump7.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump7.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

echo "dump8"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump8.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump8.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump8.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump2.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"

echo "dump9"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump9.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump9.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump9.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump9.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"

echo "dump10"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump10.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump10.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump10.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump10.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"

echo "dump11"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump11.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump11.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump11.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump11.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"


echo "dump12"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump12.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump12.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump12.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump12.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump12.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump12.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump12.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump12.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

echo "dump13"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump13.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump13.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump13.sql', MySQLDump) settings input_format_mysql_dump_table_name='fruits'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump13.sql', MySQLDump) settings input_format_mysql_dump_table_name='fruits', max_threads=1"

echo "dump14"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump14.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump14.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump14.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump14.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump14.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump14.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump14.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump14.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

echo "dump15"

$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump15.sql', MySQLDump)"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump15.sql', MySQLDump) settings max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump15.sql', MySQLDump) settings input_format_mysql_dump_table_name='test'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump15.sql', MySQLDump) settings input_format_mysql_dump_table_name='test', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump15.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump15.sql', MySQLDump) settings input_format_mysql_dump_table_name='test2', max_threads=1"
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump15.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/dump15.sql', MySQLDump) settings input_format_mysql_dump_table_name='test 3', max_threads=1"

rm $CLICKHOUSE_USER_FILES_UNIQUE/dump*.sql
