#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS --local-port 1390 "${CLICKHOUSE_URL}&query_id=my_id&query=SELECT+port+FROM+system.processes+WHERE+query_id%3D%27my_id%27+ORDER+BY+elapsed+LIMIT+1"
