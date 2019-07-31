#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="SELECT 6103"
sleep 0.1
${CLICKHOUSE_CLIENT} --query="SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} --query="SELECT count() > 0 FROM system.text_log WHERE position(system.text_log.message, 'SELECT 6103') > 0"
