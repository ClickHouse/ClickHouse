#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

user=user_$CLICKHOUSE_TEST_UNIQUE_NAME
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $user"
$CLICKHOUSE_CLIENT --query "CREATE USER $user IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"

$CLICKHOUSE_CLIENT --user $user --password hello --query "CREATE TEMPORARY TABLE table_memory_02561(name String)" 2>&1 | grep -F "Not enough privileges. To execute this query, it's necessary to have the grant CREATE TEMPORARY TABLE" > /dev/null && echo "OK"

$CLICKHOUSE_CLIENT --query "GRANT CREATE TEMPORARY TABLE ON *.* TO $user"
$CLICKHOUSE_CLIENT --query "GRANT TABLE ENGINE ON Memory TO $user"

$CLICKHOUSE_CLIENT --user $user --password hello --query "CREATE TEMPORARY TABLE table_memory_02561(name String)"

$CLICKHOUSE_CLIENT --user $user --password hello --query "CREATE TEMPORARY TABLE table_merge_tree_02561(name String) ENGINE = MergeTree() ORDER BY name" 2>&1 | grep -F "Not enough privileges. To execute this query, it's necessary to have the grant CREATE ARBITRARY TEMPORARY TABLE" > /dev/null && echo "OK"

$CLICKHOUSE_CLIENT --query "GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO $user"
$CLICKHOUSE_CLIENT --query "GRANT TABLE ENGINE ON MergeTree TO $user"

$CLICKHOUSE_CLIENT --user $user --password hello --query "CREATE TEMPORARY TABLE table_merge_tree_02561(name String) ENGINE = MergeTree() ORDER BY name"

$CLICKHOUSE_CLIENT --user $user --password hello --query "CREATE TEMPORARY TABLE table_file_02561(name String) ENGINE = File(TabSeparated)" 2>&1 | grep -F "Not enough privileges. To execute this query, it's necessary to have the grant TABLE ENGINE ON File" > /dev/null && echo "OK"

$CLICKHOUSE_CLIENT --query "GRANT FILE ON *.* TO $user"

$CLICKHOUSE_CLIENT --user $user --password hello --query "CREATE TEMPORARY TABLE table_file_02561(name String) ENGINE = File(TabSeparated)"

$CLICKHOUSE_CLIENT --user $user --password hello --query "CREATE TEMPORARY TABLE table_url_02561(name String) ENGINE = URL('http://127.0.0.1:8123?query=select+12', 'RawBLOB')" 2>&1 | grep -F "Not enough privileges. To execute this query, it's necessary to have the grant TABLE ENGINE ON URL" > /dev/null && echo "OK"

$CLICKHOUSE_CLIENT --query "GRANT URL ON *.* TO $user"

$CLICKHOUSE_CLIENT --user $user --password hello --query "CREATE TEMPORARY TABLE table_url_02561(name String) ENGINE = URL('http://127.0.0.1:8123?query=select+12', 'RawBLOB')"

$CLICKHOUSE_CLIENT --query "DROP USER $user"
