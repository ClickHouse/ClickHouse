#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `PARALLEL WITH` must not bypass the access checks of its subqueries in a `Replicated` database either.
# There a `CREATE TABLE` is additionally replayed through the replicated DDL log, and the replay of a
# statement written by the user is subject to the same rules as the replay of a direct statement.

# The user, the database and its ZooKeeper path carry a random suffix, and the leftovers of a previous
# run that died halfway are dropped up front, so that the test can be rerun on the same server.
run_id=$(random_str 8)
user="user_${CLICKHOUSE_DATABASE}_${run_id}"
db="rdb_${CLICKHOUSE_DATABASE}_${run_id}"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db} SYNC"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${db} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb_${run_id}', '1', '1')"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.* TO ${user}"
# `TABLE ENGINE` is granted up front so that the only privilege missing below is `CREATE TABLE`,
# see `05212_parallel_with_access_checks.sh`.
${CLICKHOUSE_CLIENT} --query "GRANT TABLE ENGINE ON Memory TO ${user}"

CLIENT_AS_USER="${CLICKHOUSE_CLIENT} --user ${user} --password password --distributed_ddl_output_mode none"

echo "-- without the CREATE TABLE privilege, directly"
${CLIENT_AS_USER} --query "
    CREATE TABLE ${db}.t_direct (x UInt8) ENGINE = Memory
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "ALLOWED"

echo "-- without the CREATE TABLE privilege, inside PARALLEL WITH"
${CLIENT_AS_USER} --query "
    CREATE TABLE ${db}.t_parallel_1 (x UInt8) ENGINE = Memory
    PARALLEL WITH
    CREATE TABLE ${db}.t_parallel_2 (x UInt8) ENGINE = Memory
" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "ALLOWED"

echo "-- nothing was created"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.tables
    WHERE database = '${db}' AND name IN ('t_direct', 't_parallel_1', 't_parallel_2')
"

echo "-- with the CREATE TABLE privilege, PARALLEL WITH still works"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TABLE ON ${db}.* TO ${user}"
${CLIENT_AS_USER} --query "
    CREATE TABLE ${db}.t_parallel_1 (x UInt8) ENGINE = Memory
    PARALLEL WITH
    CREATE TABLE ${db}.t_parallel_2 (x UInt8) ENGINE = Memory
"
${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.tables
    WHERE database = '${db}' AND name IN ('t_direct', 't_parallel_1', 't_parallel_2')
    ORDER BY name
"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${db} SYNC"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
