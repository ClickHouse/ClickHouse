#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

parallel_file=$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME"_parallel"
non_parallel_file=$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME"_non_parallel"

resultset_path=$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME"_template_format_resultset.tmp"
echo -ne '{prefix} \n${data}\n $$ suffix $$\n' > $resultset_path
row_path=$CUR_DIR/$CLICKHOUSE_TEST_UNIQUE_NAME"_template_format_row.tmp"
echo -ne 'x:${x:Quoted}, y:${y:Quoted}, s:${s:Quoted}' > $row_path

TEMPLATE_SETTINGS="SETTINGS format_template_resultset = '$resultset_path', format_template_row = '$row_path', format_template_rows_between_delimiter = ';\n'"

echo "Template-1"
$CLICKHOUSE_CLIENT -q "select number as x, number + 1 as y, concat('string: ', toString(number)) as s from numbers(200000) format Template $TEMPLATE_SETTINGS" --output_format_parallel_formatting=0 > $non_parallel_file
$CLICKHOUSE_CLIENT -q "select number as x, number + 1 as y, concat('string: ', toString(number)) as s from numbers(200000) format Template $TEMPLATE_SETTINGS" --output_format_parallel_formatting=1 > $parallel_file

diff $non_parallel_file $parallel_file

echo -ne '{prefix} \n${data}\n $$ suffix $$\n${totals}\n${min}\n${max}\n${rows:Quoted}\n${rows_before_limit:Quoted}\n${rows_read:Quoted}\n${bytes_read:Quoted}\n' > $resultset_path

echo "Template-2"
$CLICKHOUSE_CLIENT -q "select number as x, number + 1 as y, concat('string: ', toString(number)) as s from numbers(200000) group by number with totals order by number limit 190000 format Template $TEMPLATE_SETTINGS" --output_format_parallel_formatting=0 --extremes=1 > $non_parallel_file
$CLICKHOUSE_CLIENT -q "select number as x, number + 1 as y, concat('string: ', toString(number)) as s from numbers(200000) group by number with totals order by number limit 190000 format Template $TEMPLATE_SETTINGS" --output_format_parallel_formatting=1 --extremes=1 > $parallel_file

diff $non_parallel_file $parallel_file

rm $non_parallel_file $parallel_file
rm $resultset_path $row_path
