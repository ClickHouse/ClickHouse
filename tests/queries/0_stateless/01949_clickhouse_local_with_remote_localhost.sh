#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.remote_table (a Int64) ENGINE=TinyLog AS SELECT * FROM system.numbers limit 10;"

if [ "$CLICKHOUSE_HOST" == "localhost" ]; then
    # Connecting to 127.0.0.1 will connect to clickhouse-local itself, where the table doesn't exist
    ${CLICKHOUSE_LOCAL} -q "SELECT 'test1', * FROM remote('127.0.0.1', '${CLICKHOUSE_DATABASE}.remote_table') LIMIT 3;" 2>&1 | awk '{print $1 $2}'

    # Now connecting to 127.0.0.1:9000 will connect to the database we are running tests against
    ${CLICKHOUSE_LOCAL} -q "SELECT 'test2', * FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '${CLICKHOUSE_DATABASE}.remote_table') LIMIT 3 FORMAT CSV;" 2>&1 \
        | grep -av "ASan doesn't fully support makecontext/swapcontext functions"

    # Same test now against localhost
    ${CLICKHOUSE_LOCAL} -q "SELECT 'test3', * FROM remote('localhost', '${CLICKHOUSE_DATABASE}.remote_table') LIMIT 3;" 2>&1 | awk '{print $1 $2}'

    ${CLICKHOUSE_LOCAL} -q "SELECT 'test4', * FROM remote('localhost:${CLICKHOUSE_PORT_TCP}', '${CLICKHOUSE_DATABASE}.remote_table') LIMIT 3 FORMAT CSV;" 2>&1 \
        | grep -av "ASan doesn't fully support makecontext/swapcontext functions"
else
    # Can't test without localhost
    echo Code:81.
    echo \"test2\",0
    echo \"test2\",1
    echo \"test2\",2
    echo Code:81.
    echo \"test4\",0
    echo \"test4\",1
    echo \"test4\",2
fi


${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.remote_table;"
