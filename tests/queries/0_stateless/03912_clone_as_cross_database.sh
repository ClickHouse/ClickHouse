#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

local_dir="${CLICKHOUSE_TMP}/clone_as_cross_db_$$"

$CLICKHOUSE_LOCAL --path "$local_dir" --query "
    CREATE DATABASE ${CLICKHOUSE_DATABASE_1};
    CREATE TABLE ${CLICKHOUSE_DATABASE_1}.source_tbl (x Int8, y String) ENGINE = MergeTree PRIMARY KEY x;
    INSERT INTO ${CLICKHOUSE_DATABASE_1}.source_tbl VALUES (1, 'a'), (2, 'b'), (3, 'c');
    CREATE TABLE default.clone_target CLONE AS ${CLICKHOUSE_DATABASE_1}.source_tbl;
    SELECT * FROM default.clone_target ORDER BY x;
"

rm -rf "$local_dir"
