#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB_NAME="${CLICKHOUSE_DATABASE}"

function get_db_comment_info()
{
    $CLICKHOUSE_CLIENT --query="SHOW CREATE DATABASE ${DB_NAME};"
    $CLICKHOUSE_CLIENT --query="SELECT 'comment=', comment FROM system.databases WHERE name='${DB_NAME}'"
    echo # just a newline
}

function test_db_comments()
{
    local ENGINE_NAME="$1"
    echo "engine : ${ENGINE_NAME}"

    $CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 -m <<EOF
DROP DATABASE IF EXISTS ${DB_NAME};
CREATE DATABASE ${DB_NAME} ENGINE = ${ENGINE_NAME} COMMENT 'Test DB with comment';
EOF

    get_db_comment_info
}

test_db_comments "Memory"
test_db_comments "Atomic"
test_db_comments "Ordinary"
test_db_comments "Lazy(1)"
# test_db_comments "MySQL('127.0.0.1:9004', 'default', 'default', '')" # fails due to CH internal reasons
# test_db_comments "SQLite('dummy_sqlitedb')"
# test_db_comments "Replicated('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '1') ORDER BY k"
