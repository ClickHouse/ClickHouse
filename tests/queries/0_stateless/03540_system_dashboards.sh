#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Check that all queries from system.dashboards are correct
mapfile queries <<<"$($CLICKHOUSE_CLIENT --format LineAsString "SELECT formatQuerySingleLine(replace(query, '(default', '(test_shard_localhost')) FROM system.dashboards")"
$CLICKHOUSE_CLIENT "SYSTEM FLUSH LOGS"
for q in "${queries[@]}"; do
  $CLICKHOUSE_CLIENT --param_rounding 60 --param_seconds 600 --format Null "$q" || echo "$q" &
done
wait
