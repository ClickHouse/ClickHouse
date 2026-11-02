#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# FETCH ... ONLY works without ORDER BY; WITH TIES still requires it.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() FROM (SELECT number FROM numbers(10) OFFSET 1 ROWS FETCH FIRST 1 ROWS ONLY);" | grep -xF '1'
