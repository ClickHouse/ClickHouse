#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Synchronous logger with no log destination and one logger level raised explicitly.
$CLICKHOUSE_LOCAL --query "SELECT 1" -- --logger.async=0 --logger.levels.Application=trace

# Client logs requested with send_logs_level still arrive when the logger has no destination.
$CLICKHOUSE_LOCAL --query "SELECT 1" --send_logs_level=trace -- --logger.async=0 2>&1 >/dev/null | grep -F '<Debug> executeQuery: ' | grep -c -F 'SELECT 1'

# Records still reach system.text_log when it is the only log destination.
$CLICKHOUSE_LOCAL --query "SYSTEM FLUSH LOGS text_log; SELECT count() > 0 FROM system.text_log WHERE logger_name = 'Application'" -- --logger.async=0 --logger.levels.Application=trace --text_log=
