#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SAMPLE_FILE=$(mktemp 01947_multiple_pipe_read_sample_data_XXXXXX.tsv)

echo 'File generated:'
${CLICKHOUSE_LOCAL} -q "SELECT number AS x, if(number in (4,6), 'AAA', 'BBB') AS s from numbers(7)" > "$SAMPLE_FILE"
cat "$SAMPLE_FILE"

echo '******************'
echo 'Read twice from a regular file'
${CLICKHOUSE_LOCAL} --structure 'x UInt64, s String' -q 'select * from table; select * from table;' --file "$SAMPLE_FILE"
echo '---'
${CLICKHOUSE_LOCAL} --structure 'x UInt64, s String' -q 'select * from table WHERE x IN (select x from table);' --file "$SAMPLE_FILE"
echo '---'
${CLICKHOUSE_LOCAL} --structure 'x UInt64, s String' -q 'select * from table UNION ALL select * from table;' --file "$SAMPLE_FILE"

echo '******************'
echo 'Read twice from file descriptor that corresponds to a regular file'
${CLICKHOUSE_LOCAL} --structure 'x UInt64, s String' -q 'select * from table; select * from table;' < "$SAMPLE_FILE"
echo '---'
${CLICKHOUSE_LOCAL} --structure 'x UInt64, s String' -q 'select * from table WHERE x IN (select x from table);' < "$SAMPLE_FILE"
echo '---'
${CLICKHOUSE_LOCAL} --structure 'x UInt64, s String' -q 'select * from table UNION ALL select * from table;' < "$SAMPLE_FILE"

rm "$SAMPLE_FILE"
