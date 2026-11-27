#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Analysis of 10000 GROUP BY keys over a subquery takes far longer than the 1 s limit,
# so the query must fail during analysis, before it is planned.

COLS=$(seq 1 10000 | sed 's/.*/c& UInt8/' | paste -sd, -)
KEYS=$(seq 1 10000 | sed 's/.*/c&/' | paste -sd, -)
QUERY_ID="${CLICKHOUSE_DATABASE}_wide_group_by_$(random_str 10)"

echo "CREATE TABLE wide ($COLS) ENGINE = Memory" | $CLICKHOUSE_CLIENT

echo "SELECT count() FROM (SELECT * FROM wide) GROUP BY $KEYS FORMAT Null
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'throw', log_queries = 1" \
    | $CLICKHOUSE_CLIENT --query_id "$QUERY_ID" 2>&1 | grep -o -m1 TIMEOUT_EXCEEDED

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT type, ProfileEvents['QueryPlanBuildMicroseconds'] = 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type != 'QueryStart'"

$CLICKHOUSE_CLIENT -q "DROP TABLE wide"
