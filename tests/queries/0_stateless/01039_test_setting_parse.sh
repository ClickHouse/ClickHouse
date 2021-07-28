#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = '1G';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT value FROM system.settings WHERE name = 'max_memory_usage';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = '3Gi';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT value FROM system.settings WHERE name = 'max_memory_usage';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = '1567k';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT value FROM system.settings WHERE name = 'max_memory_usage';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = '123ki';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT value FROM system.settings WHERE name = 'max_memory_usage';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = '1567K';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT value FROM system.settings WHERE name = 'max_memory_usage';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = '123Ki';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT value FROM system.settings WHERE name = 'max_memory_usage';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = '12M';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT value FROM system.settings WHERE name = 'max_memory_usage';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = '31Mi';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT value FROM system.settings WHERE name = 'max_memory_usage';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = '1T';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT value FROM system.settings WHERE name = 'max_memory_usage';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SET max_memory_usage = '1Ti';"
$CLICKHOUSE_CLIENT --no-max-memory-usage-for-client --query "SELECT value FROM system.settings WHERE name = 'max_memory_usage';"
