#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A job queued into a `QueryRunner` table must not bypass the access checks of the principal it runs
# as. With `SQL SECURITY INVOKER` that principal is the user who inserted the job, so a user without
# the `CREATE DATABASE` privilege must not be able to create a database by queueing it.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.runner (query String) ENGINE = QueryRunner
    SETTINGS mode = 'synchronous', threads = 1 SQL SECURITY INVOKER
"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${user}"

CLIENT_AS_USER="${CLICKHOUSE_CLIENT} --user ${user} --password password"

echo "-- without the CREATE DATABASE privilege, directly"
${CLIENT_AS_USER} --query "
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_db
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "ALLOWED"

echo "-- without the CREATE DATABASE privilege, queued into the QueryRunner table"
${CLIENT_AS_USER} --query "
    INSERT INTO ${CLICKHOUSE_DATABASE}.runner VALUES ('CREATE DATABASE ${CLICKHOUSE_DATABASE}_db')
" >/dev/null 2>&1

echo "-- the database was not created"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${CLICKHOUSE_DATABASE}_db'"

echo "-- once the privilege is granted, the queued job runs"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE DATABASE ON ${CLICKHOUSE_DATABASE}_db.* TO ${user}"
${CLIENT_AS_USER} --query "
    INSERT INTO ${CLICKHOUSE_DATABASE}.runner VALUES ('CREATE DATABASE ${CLICKHOUSE_DATABASE}_db')
" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${CLICKHOUSE_DATABASE}_db'"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
