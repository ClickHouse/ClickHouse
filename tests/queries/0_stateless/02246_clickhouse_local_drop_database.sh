#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir=${CLICKHOUSE_TEST_UNIQUE_NAME}
[[ -d $dir ]] && rm -r $dir
mkdir $dir
$CLICKHOUSE_LOCAL --multiline --multiquery --path $dir -q """
DROP DATABASE IF EXISTS test;
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE TABLE test (id Int32) ENGINE=MergeTree() ORDER BY id;
DROP DATABASE test;
"""

$CLICKHOUSE_LOCAL --multiline --multiquery -q """
DROP DATABASE IF EXISTS test;
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE TABLE test (id Int32) ENGINE=MergeTree() ORDER BY id;
DROP DATABASE test;
"""
