#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

options=(
    compress=0
    compress=1
    http_wait_end_of_query=0
    http_wait_end_of_query=1
)
for option in "${options[@]}"; do
    # We are sending two queries, to make sure that when the second finished,
    # the first one will be processed completelly (i.e. record to query_log
    # will be added)
    urls=(
        "${CLICKHOUSE_URL}&query_id=$CLICKHOUSE_TEST_UNIQUE_NAME-$option&$option&query=SELECT+1+FORMAT+RowBinary"
        "${CLICKHOUSE_URL}&query_id=secondary-$CLICKHOUSE_TEST_UNIQUE_NAME-$option&$option&query=SELECT+2+FORMAT+RowBinary"
    )
    ${CLICKHOUSE_CURL} -sS "${urls[@]}" > /dev/null
done

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<< "SYSTEM FLUSH LOGS system.query_log"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<< "
    SELECT formatQuerySingleLine(query), replace(query_id, '$CLICKHOUSE_TEST_UNIQUE_NAME-', ''), ProfileEvents['NetworkSendBytes'] > 0
    FROM system.query_log
    WHERE current_database = '$CLICKHOUSE_DATABASE' AND query_id LIKE '$CLICKHOUSE_TEST_UNIQUE_NAME%' AND type != 'QueryStart'
    ORDER BY event_time_microseconds
"
