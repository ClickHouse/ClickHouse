#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_test_user_02947"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_merge_tree_index;
DROP USER IF EXISTS $user_name;

CREATE TABLE t_merge_tree_index
(
    a UInt64,
    b UInt64,
    arr Array(LowCardinality(String)),
)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS
    index_granularity = 3,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 6,
    ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_merge_tree_index (a) VALUES (1);

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
REVOKE SELECT ON $CLICKHOUSE_DATABASE.t_merge_tree_index FROM $user_name;
GRANT SELECT (b) ON $CLICKHOUSE_DATABASE.t_merge_tree_index TO $user_name;
"

$CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "SELECT a FROM t_merge_tree_index" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "OK"
$CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "SELECT arr FROM t_merge_tree_index" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "OK"
$CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "SELECT arr.size0 FROM t_merge_tree_index" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "OK"
$CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "SELECT b FROM t_merge_tree_index" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "OK"

$CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "SELECT a FROM mergeTreeIndex(currentDatabase(), t_merge_tree_index, with_marks = true)" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "OK"
$CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "SELECT a.mark FROM mergeTreeIndex(currentDatabase(), t_merge_tree_index, with_marks = true)" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "OK"
$CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "SELECT arr.mark FROM mergeTreeIndex(currentDatabase(), t_merge_tree_index, with_marks = true)" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "OK"
$CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "SELECT arr.size0.mark FROM mergeTreeIndex(currentDatabase(), t_merge_tree_index, with_marks = true)" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "OK"

$CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "SELECT b FROM mergeTreeIndex(currentDatabase(), t_merge_tree_index, with_marks = true)" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "OK"
$CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "SELECT b.mark FROM mergeTreeIndex(currentDatabase(), t_merge_tree_index, with_marks = true)" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "OK"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_merge_tree_index;
DROP USER IF EXISTS $user_name;
"
