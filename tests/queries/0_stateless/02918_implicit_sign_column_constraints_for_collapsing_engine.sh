#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

EXCEPTION_TEXT="VIOLATED_CONSTRAINT"
EXCEPTION_SUCCESS_TEXT=ok

# CollapsingMergeTree
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS collapsing_merge_tree;"
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
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not throw an exception"

# Turn off
$CLICKHOUSE_CLIENT --query="ALTER TABLE collapsing_merge_tree MODIFY SETTING add_implicit_sign_column_constraint_for_collapsing_engine=0;"

# Should succeed
$CLICKHOUSE_CLIENT --query="INSERT INTO collapsing_merge_tree SETTINGS optimize_on_insert=0 VALUES (1, 2504, 5);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM collapsing_merge_tree ORDER BY ALL;"

$CLICKHOUSE_CLIENT --query="DROP TABLE collapsing_merge_tree;"


# VersionedCollapsingMergeTree
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS versioned_collapsing_merge_tree;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE versioned_collapsing_merge_tree
(
    Key UInt32,
    Count UInt16,
    Sign Int8,
    Version UInt8
)
ENGINE=VersionedCollapsingMergeTree(Sign, Version) ORDER BY Key 
SETTINGS add_implicit_sign_column_constraint_for_collapsing_engine=1;"

# Should succeed
$CLICKHOUSE_CLIENT --query="INSERT INTO versioned_collapsing_merge_tree VALUES (1, 2504, 1, 1);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM versioned_collapsing_merge_tree;"

# Should throw an exception
$CLICKHOUSE_CLIENT --query="INSERT INTO versioned_collapsing_merge_tree VALUES (1, 2504, 5, 1);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not throw an exception"

# Turn setting off
$CLICKHOUSE_CLIENT --query="ALTER TABLE versioned_collapsing_merge_tree MODIFY SETTING add_implicit_sign_column_constraint_for_collapsing_engine=0;"

# Successful insert with invalid sign
$CLICKHOUSE_CLIENT --query="INSERT INTO versioned_collapsing_merge_tree SETTINGS optimize_on_insert=0 VALUES (1, 2505, 5, 1);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM versioned_collapsing_merge_tree ORDER BY ALL;"

$CLICKHOUSE_CLIENT --query="DROP TABLE versioned_collapsing_merge_tree;"

# Create tables without setting and turn it on after
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS collapsing_merge_tree_2;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE collapsing_merge_tree_2
(
    Key UInt32,
    Count UInt16,
    Sign Int8
)
ENGINE=CollapsingMergeTree(Sign) ORDER BY Key;"

# Successful insert with invalid sign
$CLICKHOUSE_CLIENT --query="INSERT INTO collapsing_merge_tree_2 SETTINGS optimize_on_insert=0 VALUES (1, 1, 2);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM collapsing_merge_tree_2;"

# Turn setting on
$CLICKHOUSE_CLIENT --query="ALTER TABLE collapsing_merge_tree_2 MODIFY SETTING add_implicit_sign_column_constraint_for_collapsing_engine=1;"

# Should throw an exception
$CLICKHOUSE_CLIENT --query="INSERT INTO collapsing_merge_tree_2 VALUES (1, 2504, 5);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not throw an exception"

$CLICKHOUSE_CLIENT --query="DROP TABLE collapsing_merge_tree_2;"


# VersionedCollapsingMergeTree
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS versioned_collapsing_merge_tree_2;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE versioned_collapsing_merge_tree_2
(
    Key UInt32,
    Count UInt16,
    Sign Int8,
    Version UInt8
)
ENGINE=VersionedCollapsingMergeTree(Sign, Version) ORDER BY Key;"

# Successful insert with invalid sign
$CLICKHOUSE_CLIENT --query="INSERT INTO versioned_collapsing_merge_tree_2 SETTINGS optimize_on_insert=0 VALUES (1, 2504, 2, 1);"
$CLICKHOUSE_CLIENT --query="SELECT * FROM versioned_collapsing_merge_tree_2;"

# Turn setting on
$CLICKHOUSE_CLIENT --query="ALTER TABLE versioned_collapsing_merge_tree_2 MODIFY SETTING add_implicit_sign_column_constraint_for_collapsing_engine=1;"

# Should throw an exception
$CLICKHOUSE_CLIENT --query="INSERT INTO versioned_collapsing_merge_tree_2 VALUES (1, 2505, 5, 1);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not throw an exception"

$CLICKHOUSE_CLIENT --query="DROP TABLE versioned_collapsing_merge_tree_2;"
