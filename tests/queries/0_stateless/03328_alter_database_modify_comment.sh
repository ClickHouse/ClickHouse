#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function get_database_comment_info()
{
    $CLICKHOUSE_CLIENT --query="SELECT 'name=', name, 'comment=', comment FROM system.databases where name='comment_test_database'"
    echo # just a newline
}

function test_database_comments()
{
    local ENGINE_NAME="$1"
    echo "engine : ${ENGINE_NAME}"

    $CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS comment_test_database";

    if [ "$ENGINE_NAME" = "Atomic" ]; then
        $CLICKHOUSE_CLIENT --query="CREATE DATABASE comment_test_database ENGINE = Atomic COMMENT 'Test database with comment';"
    elif [ "$ENGINE_NAME" = "Lazy" ]; then
        $CLICKHOUSE_CLIENT --query="CREATE DATABASE comment_test_database ENGINE = Lazy(1) COMMENT 'Test database with comment';"
    else
        echo "Unknown ENGINE_NAME: $ENGINE_NAME"
    fi

    echo initial comment
    get_database_comment_info

    echo change a comment
    $CLICKHOUSE_CLIENT --query="ALTER DATABASE comment_test_database MODIFY COMMENT 'new comment on a table';"
    get_database_comment_info

    echo remove a comment
    $CLICKHOUSE_CLIENT --query="ALTER DATABASE comment_test_database MODIFY COMMENT '';"
    get_database_comment_info

    echo add a comment back
    $CLICKHOUSE_CLIENT --query="ALTER DATABASE comment_test_database MODIFY COMMENT 'another comment on a table';"
    get_database_comment_info

    echo detach database
    $CLICKHOUSE_CLIENT --query="DETACH DATABASE comment_test_database SYNC;"
    get_database_comment_info

    echo re-attach database
    $CLICKHOUSE_CLIENT --query="ATTACH DATABASE comment_test_database;"
    get_database_comment_info

    echo drop database
    $CLICKHOUSE_CLIENT --query="DROP DATABASE comment_test_database";
    get_database_comment_info
}

test_database_comments "Atomic"
test_database_comments "Lazy"