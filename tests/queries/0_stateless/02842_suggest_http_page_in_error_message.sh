#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export CLICKHOUSE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}sashboards" | grep -oE "There is no handle /[^.]+\. Maybe you meant /dashboard"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}sashboard"  | grep -oE "There is no handle /[^.]+\. Maybe you meant /dashboard"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}sashboarb"  | grep -oE "There is no handle /[^.]+\. Maybe you meant /dashboard"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}sashboaxb"  | grep -oE "There is no handle /[^.]+\. Maybe you meant /dashboard"
