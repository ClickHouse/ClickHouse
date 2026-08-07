#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_02102"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_02102 (x UInt32, y String DEFAULT 'default', z Date) engine=Memory()"



$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, 'text' AS y, toDate('2020-01-01') AS z FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"

$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, 'text' AS y, toDate('2020-01-01') AS z FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, 'text' AS y, toDate('2020-01-01') AS z FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=0 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"

$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, 'text' AS y, toDate('2020-01-01') AS z FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=0 --input_format_with_types_use_header=0 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT 'text' AS y, toDate('2020-01-01') AS z, toUInt32(1) AS x FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"

$CLICKHOUSE_CLIENT -q "SELECT 'text' AS y, toDate('2020-01-01') AS z, toUInt32(1) AS x FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"

$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x FORMAT RowBinaryWithNames" | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=0 --input_format_with_names_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"

$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=0 --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, [[1, 2, 3], [4, 5], []] as a FORMAT RowBinaryWithNames" 2>&1 | $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 --input_format_with_names_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNames"  2>&1 | grep -F -c "CANNOT_SKIP_UNKNOWN_FIELD"


$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, [[1, 2, 3], [4, 5], []] as a FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02102"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02102"


$CLICKHOUSE_CLIENT -q "SELECT 'text' AS x, toDate('2020-01-01') AS y, toUInt32(1) AS z FORMAT RowBinaryWithNamesAndTypes" 2>&1 | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes" 2>&1 | grep -F -c "INCORRECT_DATA"

$CLICKHOUSE_CLIENT -q "SELECT toUInt32(1) AS x, 'text' as z, toDate('2020-01-01') AS y FORMAT RowBinaryWithNamesAndTypes" 2>&1 | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02102 FORMAT RowBinaryWithNamesAndTypes" 2>&1 | grep -F -c "INCORRECT_DATA"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_02102"
