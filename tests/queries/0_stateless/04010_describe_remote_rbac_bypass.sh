#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user04010_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE USER $user;
CREATE TABLE $db.secret_table (x UInt32, secret String) ENGINE = MergeTree ORDER BY x;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
EOF

${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE $db.secret_table; -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '$db', 'secret_table'); -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE clusterAllReplicas('test_shard_localhost', '$db', 'secret_table'); -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON $db.secret_table TO $user"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON REMOTE TO $user"

${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE $db.secret_table" | cut -f1
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '$db', 'secret_table')" | cut -f1
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE clusterAllReplicas('test_shard_localhost', '$db', 'secret_table')" | cut -f1

# Reading a table through a cluster is authorized by the privilege on its data. The user holds an
# implicit SELECT on system.one and no SHOW COLUMNS on it, which is a reachable state because an
# implicit grant carries no implied privileges.
${CLICKHOUSE_CLIENT} --user $user --query "SELECT count() FROM clusterAllReplicas('test_shard_localhost', system.one)"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', 'system', 'one')"
# Introspection keeps requiring the privilege on the schema.
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE clusterAllReplicas('test_shard_localhost', system.one); -- { serverError ACCESS_DENIED }"

# An unauthorized read is refused before the catalog is consulted, so a missing table cannot be
# told apart from one that exists.
${CLICKHOUSE_CLIENT} --user $user --query "SELECT count() FROM clusterAllReplicas('test_shard_localhost', '$db', 'no_such_table'); -- { serverError ACCESS_DENIED }"

# A column grant does not authorize reading the whole table: the check is table-level.
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(x) ON $db.secret_table TO $user"
${CLICKHOUSE_CLIENT} --user $user --query "SELECT count() FROM clusterAllReplicas('test_shard_localhost', '$db', 'secret_table'); -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
DROP TABLE IF EXISTS $db.secret_table;
EOF
