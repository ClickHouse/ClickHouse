#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database
# Tag no-parallel: the queries under test are the server-wide forms of `SYSTEM DROP REPLICA`
# and `SYSTEM DROP DATABASE REPLICA`, which enumerate every database on the server.
# Tag no-fasttest: creates a `Replicated` database.
# Tag no-replicated-database: the test controls which databases are `Replicated`,
# while with the replicated database every test database is `Replicated`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SYSTEM DROP DATABASE REPLICA` without a database name affects the whole server,
# so it must be denied for a user without privileges, exactly like `SYSTEM DROP REPLICA`.
# It should not silently succeed just because there is nothing to drop.
# At the same time it only targets `Replicated` databases, so having the privilege
# on all of them must be enough, regardless of unrelated databases.

user="user_05061_${CLICKHOUSE_DATABASE}"
rdb="rdb_05061_${CLICKHOUSE_DATABASE}"
replica="non_existing_replica_05061"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${rdb}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH no_password"

function check()
{
    local output
    if output=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "$1" 2>&1)
    then
        echo "ok"
    elif grep -q -F 'ACCESS_DENIED' <<< "${output}"
    then
        echo "denied"
    else
        echo "unexpected error: ${output}"
    fi
}

echo "-- no privileges and no Replicated databases: both forms are denied"
check "SYSTEM DROP REPLICA '${replica}'"
check "SYSTEM DROP DATABASE REPLICA '${replica}'"

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${rdb} ENGINE = Replicated('/test/${CLICKHOUSE_DATABASE}/rdb_05061', 's1', 'r1')"

echo "-- no privileges: both forms are denied"
check "SYSTEM DROP REPLICA '${replica}'"
check "SYSTEM DROP DATABASE REPLICA '${replica}'"

echo "-- privilege only on a database that is not the Replicated one: still denied"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM DROP REPLICA ON ${CLICKHOUSE_DATABASE}.* TO ${user}"
check "SYSTEM DROP DATABASE REPLICA '${replica}'"

echo "-- privilege on the Replicated database: allowed, unrelated databases do not matter"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM DROP REPLICA ON ${rdb}.* TO ${user}"
check "SYSTEM DROP DATABASE REPLICA '${replica}'"

echo "-- SYSTEM DROP REPLICA targets every database, so it is still denied"
check "SYSTEM DROP REPLICA '${replica}'"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${rdb}"

echo "-- privileges on some databases but no Replicated databases: still denied"
check "SYSTEM DROP DATABASE REPLICA '${replica}'"

echo "-- global privilege: allowed"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM DROP REPLICA ON *.* TO ${user}"
check "SYSTEM DROP DATABASE REPLICA '${replica}'"
check "SYSTEM DROP REPLICA '${replica}'"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
