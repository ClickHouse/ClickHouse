#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL_COMMAND} -I -sSg "${CLICKHOUSE_URL}?query=BADREQUEST" 2>/dev/null | grep -o 'X-ClickHouse-Exception-Code: 62'
