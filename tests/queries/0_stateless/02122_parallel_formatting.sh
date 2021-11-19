#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

parallel_file=$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME"_parallel"
non_parallel_file=$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME"_non_parallel"

formats="RowBinary RowBinaryWithNames RowBinaryWithNamesAndTypes XML Markdown Vertical Values JSONEachRow JSONStringsEachRow TSKV Pretty PrettyNoEscapes JSON JSONStrings PrettyCompact PrettyCompactNoEscapes PrettySpace PrettySpaceNoEscapes TSV TSVWithNames TSVWithNamesAndTypes CSV CSVWithNames CSVWithNamesAndTypes JSONCompactEachRow JSONCompactEachRowWithNames JSONCompactEachRowWithNamesAndTypes JSONCompactStringsEachRow JSONCompactStringsEachRowWithNames JSONCompactStringsEachRowWithNamesAndTypes"

for format in ${formats}; do
    echo $format-1
    $CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(500000) format $format" --output_format_parallel_formatting=0 --output_format_pretty_max_rows=1000000 | grep -v "elapsed" > $non_parallel_file
    $CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(500000) format $format" --output_format_parallel_formatting=1 --output_format_pretty_max_rows=1000000 | grep -v "elapsed" > $parallel_file

    diff $non_parallel_file $parallel_file

    echo $format-2
    $CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(500000) group by number with totals limit 490000 format $format" --extremes=1 --output_format_parallel_formatting=0 --output_format_pretty_max_rows=1000000 | grep -v "elapsed" > $non_parallel_file
    $CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(500000) group by number with totals limit 490000 format $format" --extremes=1 --output_format_parallel_formatting=1 --output_format_pretty_max_rows=1000000 | grep -v "elapsed" > $parallel_file

    diff $non_parallel_file $parallel_file
done

CUSTOM_SETTINGS="SETTINGS format_custom_row_before_delimiter='<row_before_delimiter>', format_custom_row_after_delimiter='<row_after_delimieter>\n', format_custom_row_between_delimiter='<row_between_delimiter>\n', format_custom_result_before_delimiter='<result_before_delimiter>\n', format_custom_result_after_delimiter='<result_after_delimiter>\n', format_custom_field_delimiter='<field_delimiter>', format_custom_escaping_rule='CSV'"

echo "CustomSeparated-1"
$CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(500000) format CustomSeparated $CUSTOM_SETTINGS" --output_format_parallel_formatting=0 > $non_parallel_file
$CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(500000) format CustomSeparated $CUSTOM_SETTINGS" --output_format_parallel_formatting=1 > $parallel_file

diff $non_parallel_file $parallel_file

echo "CustomSeparated-2"
$CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(500000) group by number with totals limit 490000 format CustomSeparated $CUSTOM_SETTINGS" --output_format_parallel_formatting=0 --extremes=1 > $non_parallel_file
$CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(500000) group by number with totals limit 490000 format CustomSeparated $CUSTOM_SETTINGS" --output_format_parallel_formatting=1 --extremes=1 > $parallel_file

diff $non_parallel_file $parallel_file

rm $non_parallel_file $parallel_file

