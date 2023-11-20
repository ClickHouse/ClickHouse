#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

EXCEPTION_SUCCESS_TEXT=ok
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS collapsing_merge_tree;"

# CollapsingSortedAlgorithm::merge() also has a check for sign column value
# optimize_on_insert = 0 is required to avoid this automatic merge behavior
$CLICKHOUSE_CLIENT --query="SET optimize_on_insert = 0;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE collapsing_merge_tree
(
    Key UInt32, 
    Count UInt16, 
    Sign Int8
)
ENGINE=CollapsingMergeTree(Sign) ORDER BY Key
SETTINGS add_implicit_sign_column_constraint_for_collapsing_engine=1;"

# Should succeed
$CLICKHOUSE_CLIENT --query="INSERT INTO collapsing_merge_tree VALUES (1, 2504, 1);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM collapsing_merge_tree;"

# Should throw an exception
$CLICKHOUSE_CLIENT --query="INSERT INTO collapsing_merge_tree VALUES (1, 2504, 5);" 2>&1 \
    | grep -q VIOLATED_CONSTRAINT && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not throw an exception"

$CLICKHOUSE_CLIENT --query="DROP TABLE collapsing_merge_tree;"
