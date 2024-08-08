#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_02099"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_02099 (number UInt64, string String, date Date) ENGINE=Memory()"

FORMATS=('TSVRaw' 'TSVRawWithNames' 'TSVRawWithNamesAndTypes' 'TabSeparatedRaw'  'TabSeparatedRawWithNames'  'TabSeparatedRawWithNamesAndTypes')

for format in "${FORMATS[@]}"
do
    echo $format
    $CLICKHOUSE_CLIENT -q "INSERT INTO test_02099 SELECT number, toString(number), toDate(number) FROM numbers(3)"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM test_02099 FORMAT $format"

    $CLICKHOUSE_CLIENT -q "SELECT * FROM test_02099 FORMAT $format" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_02099 FORMAT $format"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM test_02099"

    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02099"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE test_02099"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_nullable_02099"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_nullable_02099 ENGINE=Memory() AS SELECT number % 2 ? NULL : number from numbers(4)";

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099 FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099 FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_nullable_02099 FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099"


$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099 FORMAT TSV" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_nullable_02099 FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099 FORMAT TSVRaw" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_nullable_02099 FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_02099"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_nullable_02099"


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_nullable_string_02099"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_nullable_string_02099 (s Nullable(String)) ENGINE=Memory()";

echo 'nSome text' | $CLICKHOUSE_CLIENT -q "INSERT INTO test_nullable_string_02099 FORMAT TSVRaw"

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_nullable_string_02099"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_nullable_string_02099"
