#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CUSTOM_SETTINGS="SETTINGS format_custom_row_before_delimiter='<row_before_delimiter>', format_custom_row_after_delimiter='<row_after_delimieter>\n', format_custom_row_between_delimiter='<row_between_delimiter>\n', format_custom_result_before_delimiter='<result_before_delimiter>\n', format_custom_result_after_delimiter='<result_after_delimiter>\n', format_custom_field_delimiter='<field_delimiter>', format_custom_escaping_rule='CSV'"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_02117"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_02117 (x UInt64, y UInt64, s String) engine=Memory()"

for format in CustomSeparated CustomSeparatedWithNames CustomSeparatedWithNamesAndTypes
do
    echo $format
    $CLICKHOUSE_CLIENT -q "SELECT number AS x, number + 1 AS y, 'hello' AS s FROM numbers(5) FORMAT $format $CUSTOM_SETTINGS"
    $CLICKHOUSE_CLIENT -q "SELECT number AS x, number + 1 AS y, 'hello' AS s FROM numbers(5) FORMAT $format $CUSTOM_SETTINGS" | \
        $CLICKHOUSE_CLIENT -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT $format"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE test_02117"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_02117 (x UInt32, y String DEFAULT 'default', z Date) engine=Memory()"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, 'text' AS y, toDate('2020-01-01') AS z FORMAT CustomSeparatedWithNames $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"

$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, 'text' AS y, toDate('2020-01-01') AS z FORMAT CustomSeparatedWithNamesAndTypes $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, 'text' AS y, toDate('2020-01-01') AS z FORMAT CustomSeparatedWithNames $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_with_names_use_header=0 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"

$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, 'text' AS y, toDate('2020-01-01') AS z FORMAT CustomSeparatedWithNamesAndTypes $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_with_names_use_header=0 --input_format_with_types_use_header=0 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"


$CLICKHOUSE_CLIENT -q "SELECT 'text' AS y, toDate('2020-01-01') AS z, toUInt32(1) AS x FORMAT CustomSeparatedWithNames $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"

$CLICKHOUSE_CLIENT -q "SELECT 'text' AS y, toDate('2020-01-01') AS z, toUInt32(1) AS x FORMAT CustomSeparatedWithNamesAndTypes $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x FORMAT CustomSeparatedWithNames $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"

$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x FORMAT CustomSeparatedWithNamesAndTypes  $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x FORMAT CustomSeparatedWithNames $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=0 --input_format_with_names_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"

$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x FORMAT CustomSeparatedWithNamesAndTypes $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=0 --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, [[1, 2, 3], [4, 5], []] as a FORMAT CustomSeparatedWithNames $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 --input_format_with_names_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, [[1, 2, 3], [4, 5], []] as a FORMAT CustomSeparatedWithNamesAndTypes $CUSTOM_SETTINGS" | \
    $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02117"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02117"

TMP_FILE=$CURDIR/test_02117
$CLICKHOUSE_CLIENT -q "SELECT 'text' AS x, toDate('2020-01-01') AS y, toUInt32(1) AS z FORMAT CustomSeparatedWithNamesAndTypes $CUSTOM_SETTINGS" > $TMP_FILE
cat $TMP_FILE | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNamesAndTypes" 2>&1 | \
    grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, 'text' as z, toDate('2020-01-01') AS y FORMAT CustomSeparatedWithNamesAndTypes $CUSTOM_SETTINGS" > $TMP_FILE
cat $TMP_FILE | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02117 $CUSTOM_SETTINGS FORMAT CustomSeparatedWithNamesAndTypes" 2>&1 | \
    grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "DROP TABLE test_02117"
rm $TMP_FILE
