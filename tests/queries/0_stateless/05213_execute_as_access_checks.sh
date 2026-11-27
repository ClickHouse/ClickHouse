#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `EXECUTE AS` must not bypass access checks of the impersonated statement: the statement runs with
# the privileges of the target user, so a target without the `CREATE TABLE` privilege must be denied.

caller="caller_${CLICKHOUSE_DATABASE}"
target="target_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${caller}, ${target}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${caller} IDENTIFIED WITH plaintext_password BY 'password'"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${target}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${caller}, ${target}"
${CLICKHOUSE_CLIENT} --query "GRANT IMPERSONATE ON ${target} TO ${caller}"
# See the note in `05212_parallel_with_access_checks.sh`: `TABLE ENGINE` is granted up front so that
# the only privilege missing below is `CREATE TABLE`, otherwise the `TABLE ENGINE` check - which does
# not consult `internal` - would hide the bypass this test is about.
${CLICKHOUSE_CLIENT} --query "GRANT TABLE ENGINE ON Memory TO ${target}"

CLIENT_AS_CALLER="${CLICKHOUSE_CLIENT} --user ${caller} --password password"

# The test database may be reused across runs, so only the objects of this test are counted, and they
# are dropped both before and after the run.
tables="'t_execute_as', 't_nested_1', 't_nested_2'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_execute_as, ${CLICKHOUSE_DATABASE}.t_nested_1, ${CLICKHOUSE_DATABASE}.t_nested_2"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db"

echo "-- target has no CREATE TABLE privilege"
${CLIENT_AS_CALLER} --query "
    EXECUTE AS ${target} CREATE TABLE ${CLICKHOUSE_DATABASE}.t_execute_as (x UInt8) ENGINE = Memory
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "ALLOWED"

echo "-- target has no CREATE DATABASE privilege"
${CLIENT_AS_CALLER} --query "
    EXECUTE AS ${target} CREATE DATABASE ${CLICKHOUSE_DATABASE}_db
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "ALLOWED"

echo "-- two layers of nesting: PARALLEL WITH inside EXECUTE AS is checked too"
${CLIENT_AS_CALLER} --query "
    EXECUTE AS ${target}
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_nested_1 (x UInt8) ENGINE = Memory
    PARALLEL WITH
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_nested_2 (x UInt8) ENGINE = Memory
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "ALLOWED"

echo "-- nothing was created"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN (${tables})"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${CLICKHOUSE_DATABASE}_db'"

echo "-- once the target has the privilege, EXECUTE AS still works"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${target}"
${CLIENT_AS_CALLER} --query "
    EXECUTE AS ${target} CREATE TABLE ${CLICKHOUSE_DATABASE}.t_execute_as (x UInt8) ENGINE = Memory
"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN (${tables}) ORDER BY name"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.t_execute_as"
${CLICKHOUSE_CLIENT} --query "DROP USER ${caller}, ${target}"
