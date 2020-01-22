#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL_COMMAND} -I -sSg "${CLICKHOUSE_URL}&query=BADREQUEST" | grep -o 'X-ClickHouse-Exception-Code: 62'
