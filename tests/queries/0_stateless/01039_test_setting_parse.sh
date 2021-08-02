#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery <<EOF
SET max_memory_usage = '1G';
SELECT value FROM system.settings WHERE name = 'max_memory_usage';
EOF
$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery <<EOF
SET max_memory_usage = '3Gi';
SELECT value FROM system.settings WHERE name = 'max_memory_usage';
EOF
$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery <<EOF
SET max_memory_usage = '1567k';
SELECT value FROM system.settings WHERE name = 'max_memory_usage';
EOF
$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery <<EOF
SET max_memory_usage = '123ki';
SELECT value FROM system.settings WHERE name = 'max_memory_usage';
EOF
$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery <<EOF
SET max_memory_usage = '1567K';
SELECT value FROM system.settings WHERE name = 'max_memory_usage';
EOF
$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery <<EOF
SET max_memory_usage = '123Ki';
SELECT value FROM system.settings WHERE name = 'max_memory_usage';
EOF
$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery <<EOF
SET max_memory_usage = '12M';
SELECT value FROM system.settings WHERE name = 'max_memory_usage';
EOF
$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery <<EOF
SET max_memory_usage = '31Mi';
SELECT value FROM system.settings WHERE name = 'max_memory_usage';
EOF
$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery <<EOF
SET max_memory_usage = '1T';
SELECT value FROM system.settings WHERE name = 'max_memory_usage';
EOF
$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery <<EOF
SET max_memory_usage = '1Ti';
SELECT value FROM system.settings WHERE name = 'max_memory_usage';
EOF
