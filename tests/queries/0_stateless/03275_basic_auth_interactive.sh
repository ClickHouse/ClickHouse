#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://default:invalid_password@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"
${CLICKHOUSE_CURL} -v "$URL?query=SELECT%201" 2>&1 | grep -P '401 Unauthorized|WWW-Authenticate'
