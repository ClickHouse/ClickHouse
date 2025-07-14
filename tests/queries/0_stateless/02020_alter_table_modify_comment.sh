#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function get_table_comment_info()
{
    $CLICKHOUSE_CLIENT --query="SHOW CREATE TABLE comment_test_table;"
    $CLICKHOUSE_CLIENT --query="SELECT 'comment=', comment FROM system.tables WHERE database=currentDatabase() and name='comment_test_table'"
    echo # just a newline
}

function test_table_comments()
{
    local ENGINE_NAME="$1"
    echo "engine : ${ENGINE_NAME}"

    $CLICKHOUSE_CLIENT -m <<EOF
    DROP TABLE IF EXISTS comment_test_table;

    CREATE TABLE comment_test_table
    (
        k UInt64,
        s String
    ) ENGINE = ${ENGINE_NAME}
    COMMENT 'Test table with comment';
EOF

    echo initial comment
    get_table_comment_info

    echo change a comment
    $CLICKHOUSE_CLIENT --query="ALTER TABLE comment_test_table MODIFY COMMENT 'new comment on a table';"
    get_table_comment_info

    echo remove a comment
    $CLICKHOUSE_CLIENT --query="ALTER TABLE comment_test_table MODIFY COMMENT '';"
    get_table_comment_info

    echo add a comment back
    $CLICKHOUSE_CLIENT --query="ALTER TABLE comment_test_table MODIFY COMMENT 'another comment on a table';"
    get_table_comment_info

    echo detach table
    $CLICKHOUSE_CLIENT --query="DETACH TABLE comment_test_table SYNC;"
    get_table_comment_info

    echo re-attach table
    $CLICKHOUSE_CLIENT --query="ATTACH TABLE comment_test_table;"
    get_table_comment_info
}

test_table_comments "Null"
test_table_comments "Memory"
test_table_comments "MergeTree() ORDER BY k"
test_table_comments "Log"
test_table_comments "TinyLog"
test_table_comments "ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '1') ORDER BY k"
