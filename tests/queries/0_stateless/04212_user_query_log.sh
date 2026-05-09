#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_04212_${CLICKHOUSE_DATABASE}"
query_id="04212_user_query_log_${CLICKHOUSE_DATABASE}"
other_query_id="04212_user_query_log_other_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
}
trap cleanup EXIT

cleanup

${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.tables WHERE database = 'system' AND name = 'user_query_log'"

${CLICKHOUSE_CLIENT} --query "SELECT 1 FORMAT Null" --query_id "${other_query_id}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON system.user_query_log TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT 4212 FORMAT Null" --query_id "${query_id}"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

if ${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count() FROM system.query_log" >/dev/null 2>&1
then
    echo "UNEXPECTED"
else
    echo "query_log denied"
fi

${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT count() >= 1, countIf(user != currentUser())
    FROM system.user_query_log
    WHERE event_date >= yesterday() AND query_id = '${query_id}'"

${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT 'safe'
    FROM system.user_query_log
    WHERE event_date >= yesterday() AND throwIf(user != currentUser()) = 0
    LIMIT 1"
