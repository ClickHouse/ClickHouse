#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set +e

$CLICKHOUSE_CLIENT --receive_timeout=0 --query="SELECT sleep(2) FORMAT Null" >/dev/null 2>&1
echo $?

$CLICKHOUSE_CLIENT --receive_timeout=0 --query="SELECT sleep(2) FORMAT Null" 2>&1 | grep -q "Timeout exceeded"
echo $?
