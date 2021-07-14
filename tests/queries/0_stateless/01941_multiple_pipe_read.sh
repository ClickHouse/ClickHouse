#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SAMPLE_FILE="$CURDIR/01941_sample_data.csv"

set -e

echo 'File generated:'
${CLICKHOUSE_LOCAL} -q "SELECT number, if(number in (4,6), 'AAA', 'BBB') from numbers(7) FORMAT CSV" --format_csv_delimiter=, >"$SAMPLE_FILE"
cat "$SAMPLE_FILE"

echo '******************'
echo 'Attempt to read twice from a pipeline'
${CLICKHOUSE_LOCAL} --structure 'key String' -q 'select * from table; select * from table;' <<<foo

echo '******************'
echo 'Attempt to read twice from a regular file'
${CLICKHOUSE_LOCAL} --structure 'key String' -q 'select * from table; select * from table;' --file "$SAMPLE_FILE"

echo '******************'
echo 'Attempt to read twice from a pipe'
tpipe=$(mktemp -u)
mkfifo "$tpipe"
echo "$SAMPLE_FILE" > /tmp/pipe &
${CLICKHOUSE_LOCAL} --structure 'key String' -q 'select * from table; select * from table;' --file /tmp/pipe


rm "$SAMPLE_FILE"

