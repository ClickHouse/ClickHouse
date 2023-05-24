#!/usr/bin/env bash
# Tags: no-backward-compatibility-check
# Test fix for issue #5066

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} --include -sS "${CLICKHOUSE_URL_INTERSERVER}?endpoint=DataPartsExchange%3A%2Fclickhouse%2Ftables%2F01-01%2Fvisits%2Freplicas%2Fsome.server.com&part=0&compress=false" 2>&1 | grep -F 'HTTP/1.1 500 Internal Server Error'
