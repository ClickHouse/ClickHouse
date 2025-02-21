#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="01545_system_errors-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$query_id" -q "select query_id" &>/dev/null

retry=0
max_retries=30
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count(*) FROM system.errors WHERE query_id = '$query_id' and code = 47") -ne 1 ]]; do
  retry=$((retry+1))

  if [ $retry -ge $max_retries ]; then
    echo "Failed to query from system.error by query_id"
    break
  fi

  sleep 0.5
done

echo "OK"