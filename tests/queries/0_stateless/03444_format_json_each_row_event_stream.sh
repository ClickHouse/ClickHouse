#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT number FROM system.numbers WHERE number % 5 = 1 FORMAT JSONEachRowEventStream" 2>/dev/null | head -n29