#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_04515_${CLICKHOUSE_DATABASE}"
query_id="04515_user_query_log_${CLICKHOUSE_DATABASE}"
other_query_id="04515_user_query_log_other_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
}
trap cleanup EXIT

cleanup

${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.tables WHERE database = 'system' AND name = 'user_query_log'"

# A query from another (initiating) user, so that the query log contains rows the test user must not see.
${CLICKHOUSE_CLIENT} --query "SELECT 1 FORMAT Null" --query_id "${other_query_id}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT 4515 FORMAT Null" --query_id "${query_id}"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# The user has no grants: the query log table itself is not accessible...
if ${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count() FROM system.query_log" >/dev/null 2>&1
then
    echo "UNEXPECTED"
else
    echo "query_log denied"
fi

# ...but their own records are visible in `system.user_query_log`, and no other records are.
${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT count() >= 1, countIf(if(initial_user != '', initial_user, user) != currentUser())
    FROM system.user_query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${query_id}'"

# The exposed types do not contain LowCardinality: a LowCardinality dictionary could contain
# values from other users' rows.
${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT toTypeName(user) FROM system.user_query_log WHERE query_id = '${query_id}' LIMIT 1"

# The alias columns exposed by `system.query_log` (`ProfileEvents.Names`, `ProfileEvents.Values`,
# `Settings.Names`, `Settings.Values`) are preserved, with LowCardinality stripped from their types too.
${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT
        toTypeName(\`ProfileEvents.Names\`),
        toTypeName(\`ProfileEvents.Values\`),
        toTypeName(\`Settings.Names\`),
        toTypeName(\`Settings.Values\`)
    FROM system.user_query_log WHERE query_id = '${query_id}' LIMIT 1"

# The aliases still resolve to the corresponding map functions of the physical columns.
${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT countIf(\`ProfileEvents.Names\` != mapKeys(ProfileEvents) OR \`Settings.Values\` != mapValues(Settings))
    FROM system.user_query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${query_id}'"

# throwIf is a side channel: a predicate that observes another user's row throws. It must return
# only the current user's own rows no matter how the predicate is injected into the query.
for analyzer in 1 0
do
    ${CLICKHOUSE_CLIENT} --user "${user}" --query "
        SELECT count() >= 1
        FROM system.user_query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${query_id}'
          AND throwIf(if(initial_user != '', initial_user, user) != currentUser()) = 0
        SETTINGS enable_analyzer = ${analyzer}"

    ${CLICKHOUSE_CLIENT} --user "${user}" --query "
        SELECT count() >= 1
        FROM system.user_query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${query_id}'
        SETTINGS enable_analyzer = ${analyzer},
            additional_table_filters = {'system.query_log': 'throwIf(if(initial_user != \\'\\', initial_user, user) != currentUser()) = 0'}"

    ${CLICKHOUSE_CLIENT} --user "${user}" --query "
        SELECT count() >= 1
        FROM system.user_query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${query_id}'
        SETTINGS enable_analyzer = ${analyzer},
            additional_table_filters = {'system.user_query_log': 'throwIf(if(initial_user != \\'\\', initial_user, user) != currentUser()) = 0'}"

    # `additional_result_filter` is resolved against the result columns, so the result must contain them.
    ${CLICKHOUSE_CLIENT} --user "${user}" --query "
        SELECT user, initial_user
        FROM system.user_query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${query_id}'
        SETTINGS enable_analyzer = ${analyzer},
            additional_result_filter = 'throwIf(if(initial_user != \\'\\', initial_user, user) != currentUser()) = 0'" | sort -u | wc -l
done

# The name is taken by a system table, so it cannot be occupied by CREATE or RENAME, and it cannot be dropped.
if ${CLICKHOUSE_CLIENT} --query "CREATE TABLE system.user_query_log (x UInt8) ENGINE = Memory" >/dev/null 2>&1
then
    echo "UNEXPECTED"
else
    echo "create denied"
fi

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_04515 (x UInt8) ENGINE = Memory"
if ${CLICKHOUSE_CLIENT} --query "RENAME TABLE ${CLICKHOUSE_DATABASE}.t_04515 TO system.user_query_log" >/dev/null 2>&1
then
    echo "UNEXPECTED"
else
    echo "rename denied"
fi
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.t_04515"

if ${CLICKHOUSE_CLIENT} --query "DROP TABLE system.user_query_log" >/dev/null 2>&1
then
    echo "UNEXPECTED"
else
    echo "drop denied"
fi
