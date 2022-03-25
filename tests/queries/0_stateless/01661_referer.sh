#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT 1' --referer 'https://meta.ua/'
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} --query "SELECT http_referer FROM system.query_log WHERE current_database = currentDatabase() AND http_referer LIKE '%meta%' LIMIT 1"
