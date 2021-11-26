#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# do not print any ProfileEvents packets
$CLICKHOUSE_CLIENT -q 'select * from numbers(1e5) format Null' |& grep -c 'SelectedRows'
# print only last
$CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 -q 'select * from numbers(1e5) format Null' |& grep -o 'SelectedRows: .*$'
# print everything
test "$($CLICKHOUSE_CLIENT --print-profile-events -q 'select * from numbers(1e9) format Null' |& grep -c 'SelectedRows')" -gt 1 && echo OK || echo FAIL
# print each 100 ms
test "$($CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=100 -q 'select * from numbers(1e9) format Null' |& grep -c 'SelectedRows')" -gt 1 && echo OK || echo FAIL
