#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

timeout 3s $CLICKHOUSE_CLIENT --receive_timeout=2 -q "DROP TABLE system.query_log sync;" 2>&1 | grep -o "Timeout exceeded"
