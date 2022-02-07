#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_LOCAL --echo --multiline --multiquery -q """
SHOW TABLES;
CREATE DATABASE test1;
CREATE TABLE test1.table1 (a Int32) ENGINE=Memory;
USE test1;
SHOW TABLES;
CREATE DATABASE test2;
USE test2;
SHOW TABLES;
USE test1;
SHOW TABLES;
"""
