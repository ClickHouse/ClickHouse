#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# If the Authorization is set to "never", the credentials in the headers are ignored:
URL="${CLICKHOUSE_PORT_HTTP_PROTO}://default:invalid_password@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"
${CLICKHOUSE_CURL} -H 'Authorization: never' "$URL?query=SELECT%201"

# If the Authorization is set to "never", and the credentials are provided in URL parameters,
# the server will return 403 instead of 401 Unauthorized, so there will be no prompt in the browser.
URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?user=default&password=invalid_password"
${CLICKHOUSE_CURL} -H 'Authorization: never' -v "$URL?query=SELECT%201" 2>&1 | grep -P '403 Forbidden|ClickHouse Cloud'
