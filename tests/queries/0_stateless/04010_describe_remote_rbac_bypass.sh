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

${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE $db.secret_table" | cut -f1
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '$db', 'secret_table')" | cut -f1
${CLICKHOUSE_CLIENT} --user $user --query "DESCRIBE clusterAllReplicas('test_shard_localhost', '$db', 'secret_table')" | cut -f1

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
DROP TABLE IF EXISTS $db.secret_table;
EOF
