#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SAMPLE_FILE="$CURDIR/01941_sample_data.csv"
STD_ERROR_CAPTURED="$CURDIR/01941_std_error_captured.log"

echo 'File generated:'
${CLICKHOUSE_LOCAL} -q "SELECT number, if(number in (4,6), 'AAA', 'BBB') from numbers(7) FORMAT CSV" --format_csv_delimiter=, >"$SAMPLE_FILE"
cat "$SAMPLE_FILE"

echo '******************'
echo 'Attempt to read twice from a pipeline'
cat ${CLICKHOUSE_LOCAL} --structure 'key String' -q 'select * from table; select * from table;' <<<foo 2>"$STD_ERROR_CAPTURED"
ret = $?
echo "Return code: $ret"
if [ "$ret" -eq "0" ]; then
    echo "OK"
else
    echo "FAILED: return code is not 0"
fi

echo '******************'
./clickhouse local --structure 'key String' -q 'select * from table; select * from table;' --file /tmp/foo
echo 'Attempt to read twice from a regular file'
${CLICKHOUSE_LOCAL} --structure 'key String' -q 'select * from table; select * from table;' --file "$SAMPLE_FILE" 2>"$STD_ERROR_CAPTURED"
ret = $?
echo "Return code: $ret"
if [ "$ret" -eq "0" ]; then
    echo "OK"
else
    echo "FAILED: return code is not 0"
fi

echo '******************'
./clickhouse local --structure 'key String' -q 'select * from table; select * from table;' --file /tmp/foo
echo 'Attempt to read twice from a pipe'
mkfifo /tmp/pipe
echo "$SAMPLE_FILE" > /tmp/pipe &
${CLICKHOUSE_LOCAL} --structure 'key String' -q 'select * from table; select * from table;' --file /tmp/pipe 2>"$STD_ERROR_CAPTURED"
ret = $?
echo "Return code: $ret"
if [ "$ret" -eq "0" ]; then
    echo "FAILED"
else
    echo "OK: cannot read from pipe twice"
fi

rm "$STD_ERROR_CAPTURED" "$SAMPLE_FILE"

