#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

timeout 5 ${CLICKHOUSE_CLIENT_BINARY} --database "nonexistent" 2>&1 | grep -o "UNKNOWN_DATABASE" && echo "OK" || echo "FAIL"
