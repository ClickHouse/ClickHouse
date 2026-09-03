#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_WITH_LOGS=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=debug/g')

$CLICKHOUSE_FORMAT --query "SYSTEM FLUSH LOGS /* all tables */"
$CLICKHOUSE_FORMAT --query "SYSTEM FLUSH LOGS ON CLUSTER default"
$CLICKHOUSE_FORMAT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_FORMAT --query "SYSTEM FLUSH LOGS query_log, query_views_log"
$CLICKHOUSE_FORMAT --query "SYSTEM FLUSH LOGS ON CLUSTER default query_log"
$CLICKHOUSE_FORMAT --query "SYSTEM FLUSH LOGS ON CLUSTER default query_views_log, query_log"

OUTPUT=$(${CLICKHOUSE_CLIENT_WITH_LOGS} --query "SYSTEM FLUSH LOGS query_views_log" 2>&1)
# It should flush the requested log
echo "$OUTPUT" | grep -c "Requested flush"
echo "$OUTPUT" | grep -c "SystemLogQueue (system.query_views_log)"

# Using target table should work too
OUTPUT=$(${CLICKHOUSE_CLIENT_WITH_LOGS} --query "SYSTEM FLUSH LOGS system.query_views_log, system.part_log" 2>&1)
echo "$OUTPUT" | grep -c "Requested flush"
echo "$OUTPUT" | grep -c "SystemLogQueue (system.query_views_log)"
echo "$OUTPUT" | grep -c "SystemLogQueue (system.part_log)"

${CLICKHOUSE_CLIENT} -nm --query "SYSTEM FLUSH LOGS no_such_log_and_never_will_be -- { serverError BAD_ARGUMENTS }"
