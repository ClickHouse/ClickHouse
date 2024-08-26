#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

tmp_file="$CUR_DIR/$CLICKHOUSE_DATABASE.txt"
echo '# foo'
$CLICKHOUSE_LOCAL --engine_file_truncate_on_insert=1 -q "insert into function file('$tmp_file', 'LineAsString', 'x String') select * from input('x String') format LineAsString" <<<foo
cat "$tmp_file"
echo '# !foo'
$CLICKHOUSE_LOCAL --engine_file_truncate_on_insert=1 -q "insert into function file('$tmp_file', 'LineAsString', 'x String') select * from input('x String') where x != 'foo' format LineAsString" <<<foo
cat "$tmp_file"
echo '# bar'
$CLICKHOUSE_LOCAL --engine_file_truncate_on_insert=1 -q "insert into function file('$tmp_file', 'LineAsString', 'x String') select y from input('x String, y String') format TSV" <<<$'foo\tbar'
cat "$tmp_file"
echo '# defaults'
$CLICKHOUSE_LOCAL --input_format_tsv_empty_as_default=1 --engine_file_truncate_on_insert=1 -q "insert into function file('$tmp_file', 'LineAsString', 'x String') select y from input('x String, y String DEFAULT \\'bam\\'') format TSV" <<<$'foo\t'
cat "$tmp_file"
rm -f "${tmp_file:?}"
