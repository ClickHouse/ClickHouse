#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user="user_04402_${CLICKHOUSE_DATABASE}"
query_id="04402_user_query_log_${CLICKHOUSE_DATABASE}"
other_query_id="04402_user_query_log_other_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
}
trap cleanup EXIT

cleanup

${CLICKHOUSE_CLIENT} --query "SELECT 1 FORMAT Null" --query_id "${other_query_id}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT 4402 FORMAT Null" --query_id "${query_id}"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

if output=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT count() >= 1
    FROM system.user_query_log
    SETTINGS allow_experimental_analyzer = 1,
        additional_table_filters = {'system.query_log': 'throwIf(if(initial_user != \\'\\', initial_user, user) != currentUser()) = 0'}" 2>&1)
then
    echo "UNEXPECTED"
else
    if echo "${output}" | grep -Fq "Cannot use \`additional_table_filters\` with security barrier view \`system.user_query_log\`"
    then
        echo "additional_table_filters rejected"
    else
        echo "${output}"
        exit 1
    fi
fi
