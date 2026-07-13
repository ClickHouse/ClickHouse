#!/usr/bin/env bash
# shellcheck disable=SC2154

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

alive_host=$CLICKHOUSE_HOST
not_alive_host="255.255.255.255"

CLICKHOUSE_HOST=$not_alive_host $CLICKHOUSE_CLIENT --connect_timeout 1 --query "SELECT 1" |& grep -Fo 'Network is unreachable: 255.255.255.255:9000'
CLICKHOUSE_HOST=$alive_host $CLICKHOUSE_CLIENT --query "SELECT 1"
