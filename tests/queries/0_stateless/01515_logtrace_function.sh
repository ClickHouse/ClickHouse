#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=debug/g')

${CLICKHOUSE_CLIENT} --query="SELECT logTrace('logTrace Function Test');" 2>&1 | grep -q "logTrace Function Test" && echo "OK" || echo "FAIL"
