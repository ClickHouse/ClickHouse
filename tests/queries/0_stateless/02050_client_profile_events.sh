#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo 'do not print any ProfileEvents packets'
$CLICKHOUSE_CLIENT -q 'select * from numbers(1e5) format Null' |& grep -c 'SelectedRows'

echo 'print only last (and also number of rows to provide more info in case of failures)'
$CLICKHOUSE_CLIENT --max_block_size=65505 --print-profile-events --profile-events-delay-ms=-1 -q 'select * from numbers(1e5)' |& grep -o -e '\[ 0 \] SelectedRows: .*$' -e Exception

echo 'regression test for incorrect filtering out snapshots'
$CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 -n -q 'select 1; select 1' >& /dev/null
echo $?

echo 'regression test for overlap profile events snapshots between queries'
$CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 -n -q 'select 1; select 1' |& grep -F -o '[ 0 ] SelectedRows: 1 (increment)'

echo 'regression test for overlap profile events snapshots between queries (clickhouse-local)'
$CLICKHOUSE_LOCAL --print-profile-events --profile-events-delay-ms=-1 -n -q 'select 1; select 1' |& grep -F -o '[ 0 ] SelectedRows: 1 (increment)'

echo 'print everything'
profile_events="$($CLICKHOUSE_CLIENT --max_block_size 1 --print-profile-events -q 'select sleep(1) from numbers(2) format Null' |& grep -c 'SelectedRows')"
test "$profile_events" -gt 1 && echo OK || echo "FAIL ($profile_events)"

echo 'print each 100 ms'
profile_events="$($CLICKHOUSE_CLIENT --max_block_size 1 --print-profile-events --profile-events-delay-ms=100 -q 'select sleep(1) from numbers(2) format Null' |& grep -c 'SelectedRows')"
test "$profile_events" -gt 1 && echo OK || echo "FAIL ($profile_events)"

echo 'check that ProfileEvents is new for each query'
sleep_function_calls=$($CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 -n -q 'select sleep(1); select 1' |& grep -c 'SleepFunctionCalls')
test "$sleep_function_calls" -eq 1 && echo OK || echo "FAIL ($sleep_function_calls)"
