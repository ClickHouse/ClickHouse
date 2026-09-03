#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

q=$($CLICKHOUSE_FORMAT <<<"SELECT * REPLACE(1/3/3 AS dummy)")
echo "$q"
$CLICKHOUSE_FORMAT <<<"$q"

# multiple columns
q=$($CLICKHOUSE_FORMAT <<<"SELECT * REPLACE STRICT (1 AS id, 2 AS value) FROM (SELECT 0 id, 1 value)")
echo "$q"
$CLICKHOUSE_FORMAT <<<"$q"
