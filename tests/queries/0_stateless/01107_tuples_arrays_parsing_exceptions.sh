#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SELECT (1, 2 2)" 2>&1 | grep -o "Syntax error"
$CLICKHOUSE_CLIENT -q "SELECT [1, 2 2]" 2>&1 | grep -o "Syntax error"