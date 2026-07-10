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

# A query from another (initiating) user, so that `system.query_log` contains rows hidden by the
# security barrier view predicate for `${user}`.
${CLICKHOUSE_CLIENT} --query "SELECT 1 FORMAT Null" --query_id "${other_query_id}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT 4402 FORMAT Null" --query_id "${query_id}"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# `additional_table_filters` keyed on the inner source table (`system.query_log`) must not be applied
# inside the barrier: if it were, the `throwIf` predicate would observe the other user's hidden rows
# and throw. It must be neutralized instead, so the query returns only the current user's own rows.
for analyzer in 1 0
do
    if output=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "
        SELECT count() >= 1
        FROM system.user_query_log
        WHERE current_database = currentDatabase()
        SETTINGS allow_experimental_analyzer = ${analyzer},
            additional_table_filters = {'system.query_log': 'throwIf(if(initial_user != \\'\\', initial_user, user) != currentUser()) = 0'}" 2>&1)
    then
        if [ "${output}" = "1" ]
        then
            echo "additional_table_filters on inner source ignored (analyzer=${analyzer})"
        else
            echo "UNEXPECTED OUTPUT: ${output}"
            exit 1
        fi
    else
        echo "UNEXPECTED THROW: ${output}"
        exit 1
    fi
done

# `additional_table_filters` targeting an unrelated table joined in the outer query must keep working.
if output=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT count() >= 1
    FROM system.user_query_log AS u
    CROSS JOIN system.one AS o
    WHERE current_database = currentDatabase()
    SETTINGS allow_experimental_analyzer = 1,
        additional_table_filters = {'system.one': 'dummy = 0'}" 2>&1)
then
    echo "additional_table_filters for unrelated table accepted"
else
    echo "UNEXPECTED: ${output}"
    exit 1
fi

# `additional_table_filters` keyed on the view itself applies outside the barrier and must not be rejected.
if output=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT count() >= 1
    FROM system.user_query_log
    WHERE current_database = currentDatabase()
    SETTINGS allow_experimental_analyzer = 1,
        additional_table_filters = {'system.user_query_log': 'event_date >= toDate(0)'}" 2>&1)
then
    echo "additional_table_filters for the view accepted"
else
    echo "UNEXPECTED: ${output}"
    exit 1
fi

# `additional_result_filter` cannot be scoped to a specific table, so it is rejected outright for
# security barrier views (both analyzers).
if output=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT count() >= 1
    FROM system.user_query_log
    WHERE current_database = currentDatabase()
    SETTINGS allow_experimental_analyzer = 1,
        additional_result_filter = 'throwIf(if(initial_user != \\'\\', initial_user, user) != currentUser()) = 0'" 2>&1)
then
    echo "UNEXPECTED"
else
    if echo "${output}" | grep -Fq "Cannot use \`additional_result_filter\` with security barrier view \`system.user_query_log\`"
    then
        echo "additional_result_filter rejected"
    else
        echo "${output}"
        exit 1
    fi
fi

if output=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT count() >= 1
    FROM system.user_query_log
    WHERE current_database = currentDatabase()
    SETTINGS allow_experimental_analyzer = 0,
        additional_result_filter = 'throwIf(if(initial_user != \\'\\', initial_user, user) != currentUser()) = 0'" 2>&1)
then
    echo "UNEXPECTED"
else
    if echo "${output}" | grep -Fq "Cannot use \`additional_result_filter\` with security barrier view \`system.user_query_log\`"
    then
        echo "additional_result_filter rejected with old analyzer"
    else
        echo "${output}"
        exit 1
    fi
fi
