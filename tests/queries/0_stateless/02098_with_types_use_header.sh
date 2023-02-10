#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_02098"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_02098 (x UInt32, y String, z Date) engine=Memory()"

echo "TSVWithNamesAndTypes"
echo -e "x\ty\tz\nString\tDate\tUInt32\ntext\t2020-01-01\t1" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT TSVWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'
echo -e "y\tz\tx\nString\tDate\tUInt32\ntext\t2020-01-01\t1" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT TSVWithNamesAndTypes" && echo 'OK' || echo 'FAIL'
echo -e "x\tz\ty\nUInt32\tString\tDate\n1\ttext\t2020-01-01" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT TSVWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'

echo "CustomSeparatedWithNamesAndTypes"
echo -e "x\ty\tz\nString\tDate\tUInt32\ntext\t2020-01-01\t1" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT CustomSeparatedWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'
echo -e "y\tz\tx\nString\tDate\tUInt32\ntext\t2020-01-01\t1" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT CustomSeparatedWithNamesAndTypes" && echo 'OK' || echo 'FAIL'
echo -e "x\tz\ty\nUInt32\tString\tDate\n1\ttext\t2020-01-01" | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT CustomSeparatedWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'

echo "CSVWithNamesAndTypes"
echo -e "'x','y','z'\n'String','Date','UInt32'\n'text','2020-01-01',1" | $CLICKHOUSE_CLIENT --format_csv_allow_single_quotes=1 --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT CSVWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'
echo -e "'y','z','x'\n'String','Date','UInt32'\n'text','2020-01-01',1" | $CLICKHOUSE_CLIENT --format_csv_allow_single_quotes=1 --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT CSVWithNamesAndTypes" && echo 'OK' || echo 'FAIL'
echo -e "'x','z','y'\n'UInt32','String',Date'\n1,'text','2020-01-01'" | $CLICKHOUSE_CLIENT --format_csv_allow_single_quotes=1 --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT CSVWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'


echo "JSONCompactEachRowWithNamesAndTypes"
echo -e '["x","y","z"]\n["String","Date","UInt32"]\n["text","2020-01-01",1]' | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT JSONCompactEachRowWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'
echo -e '["y","z","x"]\n["String","Date","UInt32"]\n["text","2020-01-01",1]' | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT JSONCompactEachRowWithNamesAndTypes" && echo 'OK' || echo 'FAIL'
echo -e '["x","z","y"]\n["UInt32", "String", "Date"]\n[1, "text","2020-01-01"]' | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT JSONCompactEachRowWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'

echo "JSONCompactStringsEachRowWithNamesAndTypes"
echo -e '["x","y","z"]\n["String","Date","UInt32"]\n["text","2020-01-01","1"]' | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT JSONCompactStringsEachRowWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'
echo -e '["y","z","x"]\n["String","Date","UInt32"]\n["text","2020-01-01","1"]' | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT JSONCompactStringsEachRowWithNamesAndTypes" && echo 'OK' || echo 'FAIL'
echo -e '["x","z","y"]\n["UInt32", "String", "Date"]\n["1", "text","2020-01-01"]' | $CLICKHOUSE_CLIENT --input_format_with_names_use_header=1 --input_format_with_types_use_header=1 -q "INSERT INTO test_02098 FORMAT JSONCompactStringsEachRowWithNamesAndTypes" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -q "DROP TABLE test_02098"
