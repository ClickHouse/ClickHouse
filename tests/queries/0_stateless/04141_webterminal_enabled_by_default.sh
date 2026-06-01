#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# By default, enable_webterminal is true, so a plain GET /webterminal serves
# the HTML page with status 200.
${CLICKHOUSE_CURL} -sS -o /dev/null -w "%{http_code}\n" \
    "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/webterminal"

# HEAD requests are served the same way and must return 200.
${CLICKHOUSE_CURL} -sS -I -o /dev/null -w "%{http_code}\n" \
    "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/webterminal"

# The served body is the web terminal HTML page.
${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/webterminal" \
    | grep -o -m1 'Web Terminal'
