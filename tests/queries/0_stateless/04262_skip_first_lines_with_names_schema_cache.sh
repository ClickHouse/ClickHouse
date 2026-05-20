#!/usr/bin/env bash

# Test that skip_first_lines is included in the schema cache key for WithNames formats.
# https://github.com/ClickHouse/ClickHouse/issues/104527

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo -e 'header1,header2\na,b\n1,2' > $CLICKHOUSE_TEST_UNIQUE_NAME.csv
$CLICKHOUSE_LOCAL -m -q "
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv', CSVWithNames) SETTINGS input_format_csv_skip_first_lines=1;
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv', CSVWithNames) SETTINGS input_format_csv_skip_first_lines=0;"

echo -e 'header1\theader2\na\tb\n1\t2' > $CLICKHOUSE_TEST_UNIQUE_NAME.tsv
$CLICKHOUSE_LOCAL -m -q "
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.tsv', TSVWithNames) SETTINGS input_format_tsv_skip_first_lines=1;
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.tsv', TSVWithNames) SETTINGS input_format_tsv_skip_first_lines=0;"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.csv
rm $CLICKHOUSE_TEST_UNIQUE_NAME.tsv
