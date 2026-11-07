#!/usr/bin/env bash

# A /metrics scrape without a User-Agent header is answered.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CURL -sS -H 'User-Agent:' -o /dev/null -w '%{http_code}\n' "$CLICKHOUSE_URL_PROMETHEUS"
$CLICKHOUSE_CURL -sS -H 'User-Agent:' "$CLICKHOUSE_URL_PROMETHEUS" | grep -c '^ClickHouseProfileEvents_Query '
