#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT 1'  -H "Host: clickhouse.com"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} --query "SELECT http_host FROM system.query_log WHERE http_host LIKE '%clickhouse%' AND current_database = currentDatabase() LIMIT 1"
