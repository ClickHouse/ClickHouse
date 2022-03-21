#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# do not print any ProfileEvents packets
$CLICKHOUSE_CLIENT -q 'select * from numbers(1e5) format Null' |& grep -c 'SelectedRows'
# print only last (and also number of rows to provide more info in case of failures)
$CLICKHOUSE_CLIENT --max_block_size=65505 --print-profile-events --profile-events-delay-ms=-1 -q 'select * from numbers(1e5)' 2> >(grep -o -e '\[ 0 \] SelectedRows: .*$' -e Exception) 1> >(wc -l)
# print everything
profile_events="$($CLICKHOUSE_CLIENT --max_block_size 1 --print-profile-events -q 'select sleep(1) from numbers(2) format Null' |& grep -c 'SelectedRows')"
test "$profile_events" -gt 1 && echo OK || echo "FAIL ($profile_events)"
# print each 100 ms
profile_events="$($CLICKHOUSE_CLIENT --max_block_size 1 --print-profile-events --profile-events-delay-ms=100 -q 'select sleep(1) from numbers(2) format Null' |& grep -c 'SelectedRows')"
test "$profile_events" -gt 1 && echo OK || echo "FAIL ($profile_events)"
