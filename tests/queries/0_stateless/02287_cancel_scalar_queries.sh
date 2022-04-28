#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

timeout -k 5 -s INT 5 ${CLICKHOUSE_CLIENT} --query_id "$CLICKHOUSE_TEST_UNIQUE_NAME" --query='select (select max(number) from system.numbers) + 1;' &>/dev/null || true

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() FROM system.processes WHERE query_id = '$CLICKHOUSE_TEST_UNIQUE_NAME'"
