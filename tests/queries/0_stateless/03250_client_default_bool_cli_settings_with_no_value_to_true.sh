#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --optimize_aggregation_in_order --query "SELECT value FROM system.settings WHERE name = 'optimize_aggregation_in_order'" 2>&1 | grep -q "1" && echo "OK" || echo "FAIL"
$CLICKHOUSE_CLIENT --optimize_aggregation_in_order false --query "SELECT value FROM system.settings WHERE name = 'optimize_aggregation_in_order'" 2>&1 | grep -q "0" && echo "OK" || echo "FAIL"
