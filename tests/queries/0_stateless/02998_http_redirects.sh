#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Ping handler
${CLICKHOUSE_CURL} -s -S "${URL}/"

# A handler that is configured to return a redirect
${CLICKHOUSE_CURL} -s -S -I "${URL}/upyachka" | grep -i -P '^HTTP|Location'

# This handler is configured to not accept any query string
${CLICKHOUSE_CURL} -s -S -I "${URL}/upyachka?hello=world" | grep -i -P '^HTTP|Location'

# Check that actual redirect works
${CLICKHOUSE_CURL} -s -S -L "${URL}/upyachka"
