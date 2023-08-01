#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

parallel_file=$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME"_parallel"
non_parallel_file=$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME"_non_parallel"

CUSTOM_SETTINGS="SETTINGS format_custom_row_before_delimiter='<row_before_delimiter>', format_custom_row_after_delimiter='<row_after_delimieter>\n', format_custom_row_between_delimiter='<row_between_delimiter>\n', format_custom_result_before_delimiter='<result_before_delimiter>\n', format_custom_result_after_delimiter='<result_after_delimiter>\n', format_custom_field_delimiter='<field_delimiter>', format_custom_escaping_rule='CSV'"

echo "CustomSeparated-1"
$CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(200000) format CustomSeparated $CUSTOM_SETTINGS" --output_format_parallel_formatting=0 > $non_parallel_file
$CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(200000) format CustomSeparated $CUSTOM_SETTINGS" --output_format_parallel_formatting=1 > $parallel_file

diff $non_parallel_file $parallel_file

echo "CustomSeparated-2"
$CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(200000) group by number with totals order by number limit 190000 format CustomSeparated $CUSTOM_SETTINGS" --output_format_parallel_formatting=0 --extremes=1 > $non_parallel_file
$CLICKHOUSE_CLIENT -q "select number, number + 1, concat('string: ', toString(number)) from numbers(200000) group by number with totals order by number limit 190000 format CustomSeparated $CUSTOM_SETTINGS" --output_format_parallel_formatting=1 --extremes=1 > $parallel_file

diff $non_parallel_file $parallel_file

rm $non_parallel_file $parallel_file
