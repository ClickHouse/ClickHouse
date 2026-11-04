#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TEST_DB="${CLICKHOUSE_DATABASE}"
TEST_DB_2="${CLICKHOUSE_DATABASE}_2"

$CLICKHOUSE_CLIENT "

DROP DATABASE IF EXISTS ${TEST_DB};
CREATE DATABASE IF NOT EXISTS ${TEST_DB} ENGINE=Atomic;
CREATE TABLE ${TEST_DB}.test_table (n Int64) ENGINE=MergeTree ORDER BY n;
DETACH TABLE ${TEST_DB}.test_table;
RENAME DATABASE ${TEST_DB} TO ${TEST_DB_2};

SELECT '--- after rename';
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='${TEST_DB_2}';

ATTACH TABLE ${TEST_DB_2}.test_table;
DROP TABLE ${TEST_DB_2}.test_table SYNC;

SELECT '--- after drop';
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='${TEST_DB_2}';

DROP DATABASE ${TEST_DB_2} SYNC;

"
