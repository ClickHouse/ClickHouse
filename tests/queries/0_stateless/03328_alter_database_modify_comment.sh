#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

databasename="test_database_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function get_database_comment_info()
{
    $CLICKHOUSE_CLIENT -mq "SELECT 'name=', name, 'comment=', comment \
        FROM system.databases where name='${databasename}'"
    echo # just a newline
}

echo "databasename " ${databasename}

function test_database_comments()
{
    local ENGINE_NAME="$1"
    echo "engine : ${ENGINE_NAME}"

    $CLICKHOUSE_CLIENT -mq "DROP DATABASE IF EXISTS ${databasename}";

    if [ "$ENGINE_NAME" = "Atomic" ]; then
        $CLICKHOUSE_CLIENT -mq "CREATE DATABASE ${databasename} ENGINE = Atomic COMMENT 'Test database with comment';"
    elif [ "$ENGINE_NAME" = "Lazy" ]; then
        $CLICKHOUSE_CLIENT -mq "CREATE DATABASE ${databasename} ENGINE = Lazy(1) COMMENT 'Test database with comment';"
    elif [ "$ENGINE_NAME" = "Memory" ]; then
        $CLICKHOUSE_CLIENT -mq "CREATE DATABASE ${databasename} ENGINE = Memory COMMENT 'Test database with comment';"
    else
        echo "Unknown ENGINE_NAME: $ENGINE_NAME"
    fi

    echo initial comment
    get_database_comment_info

    echo change a comment
    $CLICKHOUSE_CLIENT -mq "ALTER DATABASE ${databasename} MODIFY COMMENT 'new comment on database';"
    get_database_comment_info

    echo add a comment back
    $CLICKHOUSE_CLIENT -mq "ALTER DATABASE ${databasename} MODIFY COMMENT 'another comment on database';"
    get_database_comment_info

    echo detach database
    $CLICKHOUSE_CLIENT -mq "DETACH DATABASE ${databasename} SYNC;"
    get_database_comment_info

    echo re-attach database
    $CLICKHOUSE_CLIENT -mq "ATTACH DATABASE ${databasename};"
    get_database_comment_info

    echo drop database
    $CLICKHOUSE_CLIENT -mq "DROP DATABASE ${databasename};"
    get_database_comment_info
}

test_database_comments "Atomic"
test_database_comments "Lazy"
test_database_comments "Memory"