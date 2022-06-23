#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


for _ in $(seq 1 10); do
    ${CLICKHOUSE_CLIENT} -q "select count() > 1 as ok from (select * from odbc('DSN={ClickHouse DSN (ANSI)}','system','tables'))" 2>/dev/null && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "select count() > 1 as ok from (select * from odbc('DSN={ClickHouse DSN (Unicode)}','system','tables'))"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t (x UInt8, y Float32, z String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t VALUES (1,0.1,'a я'),(2,0.2,'b ą'),(3,0.3,'c d')"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM odbc('DSN={ClickHouse DSN (ANSI)}','$CLICKHOUSE_DATABASE','t') ORDER BY x"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM odbc('DSN={ClickHouse DSN (Unicode)}','$CLICKHOUSE_DATABASE','t') ORDER BY x"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
