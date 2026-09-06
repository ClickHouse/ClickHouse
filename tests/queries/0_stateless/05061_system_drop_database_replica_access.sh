#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SYSTEM DROP DATABASE REPLICA` without a database name affects the whole server,
# so it must be denied for a user without privileges, exactly like `SYSTEM DROP REPLICA`.
# It should not silently succeed just because there is nothing to drop.

user="user_05061_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH no_password"

function check_denied()
{
    if ${CLICKHOUSE_CLIENT} --user "${user}" --query "$1" 2>&1 | grep -q -F "$2"
    then
        echo "denied"
    else
        echo "allowed"
    fi
}

check_denied "SYSTEM DROP REPLICA 'non_existing_replica_05061'" \
    'Access denied for SYSTEM DROP REPLICA. Not enough permissions to drop these databases:'

check_denied "SYSTEM DROP DATABASE REPLICA 'non_existing_replica_05061'" \
    'Access denied for SYSTEM DROP DATABASE REPLICA. Not enough permissions to drop these databases:'

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
