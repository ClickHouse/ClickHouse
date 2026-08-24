#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary "SELECT formatRow('JSONEachRow', number) as test FROM (SELECT number FROM numbers(15))"
