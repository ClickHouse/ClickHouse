#!/usr/bin/env bash

# For a user without global SHOW TABLES, a query on system.tables reading only
# the name/database columns must not skip databases the user is granted on.
# The query must see all databases, granted and not: filtering by table name
# only is what triggers the bug.
# Suppress style check: database=$CLICKHOUSE_DATABASE

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db="${CLICKHOUSE_DATABASE}"
tbl="t_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"

for i in 1 2 3; do
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db}_b0${i}"
    ${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${db}_b0${i}"
    ${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${db}_b0${i}.${tbl} (x UInt8) ENGINE = MergeTree ORDER BY x"
done

${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW, SELECT ON ${db}.* TO ${user}"
for i in 1 2 3; do
    ${CLICKHOUSE_CLIENT} -q "GRANT SHOW, SELECT ON ${db}_b0${i}.* TO ${user}"
done

# Reading only name (count) or database+name takes the fast path.
${CLICKHOUSE_CLIENT} --user "${user}" --password hello -q "SELECT count() FROM system.tables WHERE table = '${tbl}'"
${CLICKHOUSE_CLIENT} --user "${user}" --password hello -q "SELECT right(database, 3) FROM system.tables WHERE table = '${tbl}' ORDER BY database"

# Control: referencing `engine` disables the fast path.
${CLICKHOUSE_CLIENT} --user "${user}" --password hello -q "SELECT right(database, 3) FROM system.tables WHERE table = '${tbl}' AND engine LIKE '%MergeTree%' ORDER BY database"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
for i in 1 2 3; do
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${db}_b0${i}"
done
