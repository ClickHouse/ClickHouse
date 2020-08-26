#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

clickhouse_client_removed_host_parameter --host="${CLICKHOUSE_HOST}" --query="SELECT 1";
clickhouse_client_removed_host_parameter --host "${CLICKHOUSE_HOST}" --query "SELECT 1";
clickhouse_client_removed_host_parameter -h"${CLICKHOUSE_HOST}" -q"SELECT 1";
