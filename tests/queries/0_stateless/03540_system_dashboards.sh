#!/usr/bin/env bash
# Tags: no-parallel, no-flaky-check
# - no-parallel - can be quite intense
# - no-flaky-check - too slow under ASan

# Check that all queries from system.dashboards are correct

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# - replace default with test_shard_localhost (CI does not have default cluster)
# - replace log_ with log$ to avoid reading all tables matching $log_ pattern, since this also includes distributed tables from ci-logs
mapfile queries <<<"$($CLICKHOUSE_CURL -sSk "${CLICKHOUSE_URL}&default_format=LineAsString" -d "SELECT encodeURLComponent(formatQuerySingleLine(replace(replace(query, '(default', '(test_shard_localhost'), '_log', '_log$'))) FROM system.dashboards")"
$CLICKHOUSE_CURL -sSk "${CLICKHOUSE_URL}" -d "SYSTEM FLUSH LOGS"
for q in "${queries[@]}"; do
  echo "${CLICKHOUSE_URL}&param_rounding=60&param_seconds=600&serialize_query_plan=0&default_format=Null&query=$q"
done | {
  # - -n1 - we need real parallellism
  xargs -n1 -P10 $CLICKHOUSE_CURL -sSk
}
