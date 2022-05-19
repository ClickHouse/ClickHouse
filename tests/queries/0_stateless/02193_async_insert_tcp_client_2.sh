#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_insert_02193_2"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_insert_02193_2 (id UInt32, s String) ENGINE = Memory"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_async_insert_02193_2 SETTINGS async_insert = 1 FORMAT CSV 1,aaa"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_async_insert_02193_2 SETTINGS async_insert = 1 FORMAT Values (2, 'bbb')"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_async_insert_02193_2 VALUES (3, 'ccc')" --async_insert=1
${CLICKHOUSE_CLIENT} -q 'INSERT INTO t_async_insert_02193_2 FORMAT JSONEachRow {"id": 4, "s": "ddd"}' --async_insert=1

${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_async_insert_02193_2 ORDER BY id"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t_async_insert_02193_2"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_insert_02193_2"
