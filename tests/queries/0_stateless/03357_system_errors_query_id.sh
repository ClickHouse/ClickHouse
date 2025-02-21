#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="01545_system_errors-$CLICKHOUSE_DATABASE"
query_id1="01546_system_errors-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$query_id" -q "select query_id;" &>/dev/null

$CLICKHOUSE_CLIENT -q "
  SELECT count(*)
  FROM system.errors
  WHERE query_id = '$query_id' and code = 47;
"
