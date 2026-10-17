#!/usr/bin/env bash
# shellcheck disable=SC2154

# A set port adds `--port` to `$CLICKHOUSE_CLIENT`, and then the client ignores `CLICKHOUSE_HOST`.
unset CLICKHOUSE_PORT_TCP

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

alive_host=$CLICKHOUSE_HOST
not_alive_host="255.255.255.255"

# The errno text before the address differs per OS (e.g. "Network is unreachable" on Linux,
# "Address family not supported" on macOS); match only the address, which is reported exactly once.
CLICKHOUSE_HOST=$not_alive_host $CLICKHOUSE_CLIENT --connect_timeout 1 --query "SELECT 1" |& grep -Fo '255.255.255.255:9000'
CLICKHOUSE_HOST=$alive_host $CLICKHOUSE_CLIENT --query "SELECT 1"
