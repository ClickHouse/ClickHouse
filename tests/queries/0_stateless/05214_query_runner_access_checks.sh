#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A job queued into a `QueryRunner` table must not bypass the access checks of the principal it runs
# as. With `SQL SECURITY INVOKER` that principal is the user who inserted the job, so a user without
# the `CREATE DATABASE` privilege must not be able to create a database by queueing it.
#
# The `INSERT` into the table succeeds no matter how the job ends - a failed job is only logged - and
# the mere absence of the database would also be observed if the job never ran at all. Hence the
# outcome of every job is asserted through `system.query_log`, where the jobs are recorded as
# internal queries of the `ClickHouse QueryRunner` client running on behalf of the user.

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}_db"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.runner (query String) ENGINE = QueryRunner
    SETTINGS mode = 'synchronous', threads = 1 SQL SECURITY INVOKER
"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.outer_runner (query String) ENGINE = QueryRunner
    SETTINGS mode = 'synchronous', threads = 1 SQL SECURITY INVOKER
"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${user}"

CLIENT_AS_USER="${CLICKHOUSE_CLIENT} --user ${user} --password password"

# Prints how the jobs whose query text starts with $1 ended: the log entry type and the error code.
# Only the jobs of this test's user run by the `QueryRunner` client are considered, so the direct
# queries of the user and the `INSERT` queries that queued the jobs are not mixed in.
function jobs_outcome()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT type, errorCodeToName(exception_code)
        FROM system.query_log
        WHERE event_date >= yesterday() AND is_internal AND client_name = 'ClickHouse QueryRunner'
            AND user = '${user}' AND startsWith(query, '${1}') AND type != 'QueryStart'
        ORDER BY event_time_microseconds
    "
}

echo "-- without the CREATE DATABASE privilege, directly"
${CLIENT_AS_USER} --query "CREATE DATABASE ${db}" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "ALLOWED"

echo "-- without the CREATE DATABASE privilege, queued into the QueryRunner table: the job is denied"
${CLIENT_AS_USER} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.runner VALUES ('CREATE DATABASE ${db}')"
jobs_outcome "CREATE DATABASE ${db}"

echo "-- queued through two layers of QueryRunner tables: the inner job is denied as well"
${CLIENT_AS_USER} --query "
    INSERT INTO ${CLICKHOUSE_DATABASE}.outer_runner
    VALUES ('INSERT INTO ${CLICKHOUSE_DATABASE}.runner VALUES (''CREATE DATABASE ${db}'')')
"
jobs_outcome "INSERT INTO ${CLICKHOUSE_DATABASE}.runner"
jobs_outcome "CREATE DATABASE ${db}"

echo "-- the database was not created"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${db}'"

echo "-- once the privilege is granted, the queued job runs"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE DATABASE ON ${db}.* TO ${user}"
${CLIENT_AS_USER} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.runner VALUES ('CREATE DATABASE ${db}')"
jobs_outcome "CREATE DATABASE ${db}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${db}'"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
