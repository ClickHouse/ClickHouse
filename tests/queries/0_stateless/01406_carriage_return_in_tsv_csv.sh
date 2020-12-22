#!/usr/bin/env bash
# shellcheck disable=SC2028

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

echo 'CSVWithNames'
echo -n $'key\r\nbar\r\n' | ${CLICKHOUSE_LOCAL} --input-format CSVWithNames -S 'key String' -q 'select * from table'

echo 'TSVWithNames'
echo -n $'key\r\nbar\r\n' | ${CLICKHOUSE_LOCAL} --input-format TSVWithNames -S 'key String' -q 'select * from table' >& /dev/null
echo $?
echo -n $'key\\r\nbar\n' | ${CLICKHOUSE_LOCAL} --input_format_skip_unknown_fields=1 --input-format TSVWithNames -S 'key String' -q 'select * from table'

echo 'TSV'
echo -n $'key\r\nbar\r\n' | ${CLICKHOUSE_LOCAL} --input-format TSV -S 'key String' -q 'select * from table' >& /dev/null
echo $?
echo -n $'key\\r\nbar\n' | ${CLICKHOUSE_LOCAL} --input-format TSV -S 'key String' -q 'select * from table'
