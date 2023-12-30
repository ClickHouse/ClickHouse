#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SHOW PROCESSLIST" &>/dev/null
$CLICKHOUSE_CLIENT -q "SHOW DATABASES" &>/dev/null
$CLICKHOUSE_CLIENT -q "SHOW TABLES" &>/dev/null
$CLICKHOUSE_CLIENT -q "SHOW ENGINES" &>/dev/null
$CLICKHOUSE_CLIENT -q "SHOW FUNCTIONS" &>/dev/null
$CLICKHOUSE_CLIENT -q "SHOW MERGES" &>/dev/null
