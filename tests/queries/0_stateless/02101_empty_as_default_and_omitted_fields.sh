#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_02101"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_02101 (x UInt64, y UInt64 DEFAULT 42) ENGINE=Memory()"

echo 'TSV'
echo -e 'x\ty\n1\t' | $CLICKHOUSE_CLIENT --input_format_tsv_empty_as_default=1 --input_format_defaults_for_omitted_fields=1 -q "INSERT INTO test_02101 FORMAT TSVWithNames"
echo -e 'x\ty\n2\t' | $CLICKHOUSE_CLIENT --input_format_tsv_empty_as_default=1 --input_format_defaults_for_omitted_fields=0 -q "INSERT INTO test_02101 FORMAT TSVWithNames"
echo -e 'x\tz\n3\t123' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --input_format_skip_unknown_fields=1 -q "INSERT INTO test_02101 FORMAT TSVWithNames"
echo -e 'x\tz\n4\t123' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=0 --input_format_skip_unknown_fields=1 -q "INSERT INTO test_02101 FORMAT TSVWithNames"

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02101 ORDER BY x"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02101"

echo 'CSV'
echo -e '"x","y"\n1,' | $CLICKHOUSE_CLIENT --input_format_csv_empty_as_default=1 --input_format_defaults_for_omitted_fields=1 -q "INSERT INTO test_02101 FORMAT CSVWithNames"
echo -e '"x","y"\n2,' | $CLICKHOUSE_CLIENT --input_format_csv_empty_as_default=1 --input_format_defaults_for_omitted_fields=0 -q "INSERT INTO test_02101 FORMAT CSVWithNames"
echo -e '"x","z"\n3,123' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --input_format_skip_unknown_fields=1 -q "INSERT INTO test_02101 FORMAT CSVWithNames"
echo -e '"x","z"\n4,123' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=0 --input_format_skip_unknown_fields=1 -q "INSERT INTO test_02101 FORMAT CSVWithNames"

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02101 ORDER BY x"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02101"

echo 'JSONEachRow'
echo -e '{"x" : 1, "z" : 123}' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --input_format_skip_unknown_fields=1 -q "INSERT INTO test_02101 FORMAT JSONEachRow"
echo -e '{"x" : 2, "z" : 123}' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=0 --input_format_skip_unknown_fields=1 -q "INSERT INTO test_02101 FORMAT JSONEachRow"

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02101 ORDER BY x"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE test_02101"

echo 'JSONCompactEachRow'
echo -e '["x", "z"], [1, 123]' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --input_format_skip_unknown_fields=1 -q "INSERT INTO test_02101 FORMAT JSONCompactEachRowWithNames"
echo -e '["x", "z"], [2, 123]' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=0 --input_format_skip_unknown_fields=1 -q "INSERT INTO test_02101 FORMAT JSONCompactEachRowWithNames"

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02101 ORDER BY x"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_02101"

