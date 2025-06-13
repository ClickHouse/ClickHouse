#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

options=(
    compress=0
    compress=1
    wait_end_of_query=0
    wait_end_of_query=1
)
for option in "${options[@]}"; do
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=$CLICKHOUSE_TEST_UNIQUE_NAME-$option&$option" -d @- <<< "SELECT 1 FORMAT RowBinary" > /dev/null
done

# HTTPHandler will finish http request before query_log entry about query finish will be created.
# That is why here we need to wait for all queries to finish.
for _ in {1..60}; do
    finished_queries_count=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.query_log
        WHERE current_database = '$CLICKHOUSE_DATABASE' AND query_id LIKE '$CLICKHOUSE_TEST_UNIQUE_NAME%' AND type != 'QueryStart'
    ")

    if [[ $finished_queries_count -ne "4" ]]; then
        sleep 0.1
    else
        break
    fi
done

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<< "SYSTEM FLUSH LOGS system.query_log"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<< "
    SELECT query, replace(query_id, '$CLICKHOUSE_TEST_UNIQUE_NAME-', ''), ProfileEvents['NetworkSendBytes'] > 0
    FROM system.query_log
    WHERE current_database = '$CLICKHOUSE_DATABASE' AND query_id LIKE '$CLICKHOUSE_TEST_UNIQUE_NAME%' AND type != 'QueryStart'
    ORDER BY event_time_microseconds
"
