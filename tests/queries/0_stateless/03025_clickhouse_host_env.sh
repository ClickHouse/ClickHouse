#!/usr/bin/env bash
# shellcheck disable=SC2154

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

alive_host=$CLICKHOUSE_HOST
not_alive_host="10.100.0.0"

export CLICKHOUSE_HOST=$not_alive_host
error="$($CLICKHOUSE_CLIENT --connect_timeout 1 --query "SELECT 1" 2>&1 > /dev/null)"
echo "${error}" | grep -Fc "DB::NetException"
echo "${error}" | grep -Fc "${CLICKHOUSE_HOST}"

export CLICKHOUSE_HOST=$alive_host
$CLICKHOUSE_CLIENT -q "SELECT 1"
