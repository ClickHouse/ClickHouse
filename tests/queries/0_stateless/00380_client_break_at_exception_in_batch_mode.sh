#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery --query="SELECT 1; SELECT xyz; SELECT 2;" 2> /dev/null || true;
