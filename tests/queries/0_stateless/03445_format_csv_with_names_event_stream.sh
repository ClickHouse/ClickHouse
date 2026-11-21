#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT number, (number+1337)%228 FROM system.numbers WHERE number % 5 = 1 FORMAT CSVWithNamesEventStream" 2>/dev/null | head -n29
