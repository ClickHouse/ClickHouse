#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# $CLICKHOUSE_CLIENT -mn -q "SET create_if_not_exists=0;"  # Default
$CLICKHOUSE_CLIENT -mn -q "
DROP TABLE IF EXISTS example_table;
CREATE TABLE example_table (id UInt32) ENGINE=MergeTree() ORDER BY id;
CREATE TABLE example_table (id UInt32) ENGINE=MergeTree() ORDER BY id;
" 2> /dev/null
# ensure failed error code
echo $?
$CLICKHOUSE_CLIENT -mn -q "
DROP DATABASE IF EXISTS example_database;
CREATE DATABASE example_database;
CREATE DATABASE example_database;
" 2> /dev/null
echo $?

$CLICKHOUSE_CLIENT -mn -q "
SET create_if_not_exists=1;
DROP TABLE IF EXISTS example_table;
CREATE TABLE example_table (id UInt32) ENGINE=MergeTree() ORDER BY id;
CREATE TABLE example_table (id UInt32) ENGINE=MergeTree() ORDER BY id;
"
# ensure successful error code
echo $?


$CLICKHOUSE_CLIENT -mn -q "
SET create_if_not_exists=1;
DROP DATABASE IF EXISTS example_database;
CREATE DATABASE example_database;
CREATE DATABASE example_database;
"
echo $?

$CLICKHOUSE_CLIENT -mn -q "
DROP DATABASE example_database;
DROP TABLE example_table;
"