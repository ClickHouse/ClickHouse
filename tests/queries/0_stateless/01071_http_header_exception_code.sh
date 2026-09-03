#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if [[ $(${CLICKHOUSE_CURL_COMMAND} -q -I "${CLICKHOUSE_URL}&query=BADREQUEST" 2>&1 | grep -c 'X-ClickHouse-Exception-Code: 62') -eq 1 ]]; then
    echo "True"
fi
