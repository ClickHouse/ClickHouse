#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_insert_dup_ident"
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_insert_dup_ident
(
    date Date,
    name String,
    category String,
    tag String,
    value Float64,
    version Int64,
    flag Int8,
    count UInt64,
    id Int64
)
ENGINE = MergeTree
ORDER BY (date, version)
"

printf 'name,category,value\n"alice","fruit",3.14\n' | ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_insert_dup_ident SELECT toDate('2021-05-07') date, name, category, category, value, 1, 1, 1, 42 FROM input('name String, category String, value Float64') FORMAT CSVWithNames"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_insert_dup_ident"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_insert_dup_ident"
