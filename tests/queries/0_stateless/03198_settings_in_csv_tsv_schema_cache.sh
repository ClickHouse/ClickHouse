#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo -e 'a,b,c\n1,2,3' > $CLICKHOUSE_TEST_UNIQUE_NAME.csv
$CLICKHOUSE_LOCAL -m -q "
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv') SETTINGS input_format_csv_skip_first_lines=1;
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv') SETTINGS input_format_csv_skip_first_lines=0;
SELECT count() from system.schema_inference_cache where format = 'CSV' and additional_format_info like '%skip_first_lines%';"

echo -e 'a,b,c\n"1",2,3' > $CLICKHOUSE_TEST_UNIQUE_NAME.csv
$CLICKHOUSE_LOCAL -m -q "
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv') SETTINGS input_format_csv_try_infer_numbers_from_strings=1;
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv') SETTINGS input_format_csv_try_infer_numbers_from_strings=0;
SELECT count() from system.schema_inference_cache where format = 'CSV' and additional_format_info like '%try_infer_numbers_from_strings%';"

echo -e 'a,b,c\n"(1,2,3)",2,3' > $CLICKHOUSE_TEST_UNIQUE_NAME.csv
$CLICKHOUSE_LOCAL -m -q "
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv') SETTINGS input_format_csv_try_infer_strings_from_quoted_tuples=1;
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv') SETTINGS input_format_csv_try_infer_strings_from_quoted_tuples=0;
SELECT count() from system.schema_inference_cache where format = 'CSV' and additional_format_info like '%try_infer_strings_from_quoted_tuples%';"

echo -e 'a\tb\tc\n1\t2\t3' > $CLICKHOUSE_TEST_UNIQUE_NAME.tsv
$CLICKHOUSE_LOCAL -m -q "
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.tsv') SETTINGS input_format_tsv_skip_first_lines=1;
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.tsv') SETTINGS input_format_tsv_skip_first_lines=0;
SELECT count() from system.schema_inference_cache where format = 'TSV' and additional_format_info like '%skip_first_lines%';"


rm $CLICKHOUSE_TEST_UNIQUE_NAME.csv
rm $CLICKHOUSE_TEST_UNIQUE_NAME.tsv

