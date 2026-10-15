#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `PARALLEL WITH` must not bypass access checks of its subqueries.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user}"
# `TABLE ENGINE` is granted up front so that the only privilege missing below is `CREATE TABLE`.
# `access_control_improvements.table_engines_require_grant` is enabled in the test configuration, and
# the `TABLE ENGINE` check in `getTablePropertiesAndNormalizeCreateQuery` does not consult `internal` -
# without this grant a `CREATE TABLE` would be refused by that check even when its own access check is
# skipped, which would hide the very bypass this test is about.
${CLICKHOUSE_CLIENT} --query "GRANT TABLE ENGINE ON Memory TO ${user}"

CLIENT_AS_USER="${CLICKHOUSE_CLIENT} --user ${user} --password password"

# The test database may be reused across runs, so only the objects of this test are counted, and they
# are dropped both before and after the run.
tables="'t_direct', 't_parallel_1', 't_parallel_2'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_direct, ${CLICKHOUSE_DATABASE}.t_parallel_1, ${CLICKHOUSE_DATABASE}.t_parallel_2"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db_1"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db_2"

echo "-- without the CREATE TABLE privilege, directly"
${CLIENT_AS_USER} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_direct (x UInt8) ENGINE = Memory
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "ALLOWED"

echo "-- without the CREATE TABLE privilege, inside PARALLEL WITH"
${CLIENT_AS_USER} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_parallel_1 (x UInt8) ENGINE = Memory
    PARALLEL WITH
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_parallel_2 (x UInt8) ENGINE = Memory
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "ALLOWED"

echo "-- CREATE DATABASE inside PARALLEL WITH is checked too"
${CLIENT_AS_USER} --query "
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_db_1
    PARALLEL WITH
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_db_2
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "ALLOWED"

echo "-- nothing was created"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN (${tables})"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name IN ('${CLICKHOUSE_DATABASE}_db_1', '${CLICKHOUSE_DATABASE}_db_2')"

echo "-- with the CREATE TABLE privilege, PARALLEL WITH still works"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${user}"
${CLIENT_AS_USER} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_parallel_1 (x UInt8) ENGINE = Memory
    PARALLEL WITH
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_parallel_2 (x UInt8) ENGINE = Memory
"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN (${tables}) ORDER BY name"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.t_parallel_1, ${CLICKHOUSE_DATABASE}.t_parallel_2"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
