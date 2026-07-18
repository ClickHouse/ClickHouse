#!/usr/bin/env bash
# shellcheck disable=SC2154

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

alive_host=$CLICKHOUSE_HOST
not_alive_host="255.255.255.255"

# The errno text before the address differs per OS (e.g. "Network is unreachable" on Linux,
# "Address family not supported" on macOS); match only the host:port that ClickHouse always names.
# When neither the port nor the TLS mode is given, the client probes both the plain (9000) and the
# secure default port, so it can name the address more than once; `sort -u` collapses that to one line.
CLICKHOUSE_HOST=$not_alive_host $CLICKHOUSE_CLIENT --connect_timeout 1 --query "SELECT 1" |& grep -Fo '255.255.255.255:9000' | sort -u
CLICKHOUSE_HOST=$alive_host $CLICKHOUSE_CLIENT --query "SELECT 1"
