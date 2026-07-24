#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# if it will not match, the exit code of grep will be non-zero and the test will fail
$CLICKHOUSE_CLIENT -q "SELECT toDateTime(format('{}-{}-01 00:00:00', '2021', '1'))" |& grep -F -q "Cannot parse string '2021-1-01 00:00:00' as DateTime"
