#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# grep all fields, that should be set for CORS support
$CLICKHOUSE_CURL "${CLICKHOUSE_URL}" -X OPTIONS -vs 2>&1 | grep -E "HTTP/1.1 204 No Content|Access-Control-Allow-Origin|Access-Control-Allow-Headers|Access-Control-Allow-Methods|Access-Control-Max-Age"

# grep all fields, that should be set for CORS support
echo 'SELECT 1' | $CLICKHOUSE_CURL -X POST -H 'Origin: clickhouse-test' "${CLICKHOUSE_URL}&password=wrong_password" --data @- -vs 2>&1 | grep -E "HTTP/1.1 401 Unauthorized|HTTP/1.1 403 Forbidden|Access-Control-Allow-Origin|Access-Control-Allow-Headers|Access-Control-Allow-Methods|Access-Control-Max-Age"
