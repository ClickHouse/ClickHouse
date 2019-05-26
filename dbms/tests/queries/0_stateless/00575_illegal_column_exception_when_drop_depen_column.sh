#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


exception_pattern="Code: 44.*Cannot drop column id, because column id2 depends on it"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (dt Date DEFAULT now(), id UInt32, id2 UInt32 DEFAULT id + 1) ENGINE = MergeTree(dt, dt, 8192);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test(dt,id) VALUES ('2018-02-22',3), ('2018-02-22',4), ('2018-02-22',5);"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test ORDER BY id;"
echo `${CLICKHOUSE_CLIENT} --query "ALTER TABLE test DROP COLUMN id;" --server_logs_file=/dev/null 2>&1 | grep -c "$exception_pattern"`
${CLICKHOUSE_CLIENT} --query "ALTER TABLE test DROP COLUMN id2;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test ORDER BY id;"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE test DROP COLUMN id;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test ORDER BY dt"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test;"
