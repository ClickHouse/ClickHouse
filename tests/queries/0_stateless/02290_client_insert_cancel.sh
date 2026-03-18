#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

yes 1 | $CLICKHOUSE_CLIENT --query_id "$CLICKHOUSE_TEST_UNIQUE_NAME" -q "insert into function null('n Int') format TSV" &
client_pid=$!

# wait for the query
while [ "$($CLICKHOUSE_CLIENT -q "select count() from system.processes where query_id = '$CLICKHOUSE_TEST_UNIQUE_NAME'")" = 0 ]; do
    sleep 0.1
done

kill -INT $client_pid
wait $client_pid
# if client does not cancel it properly (i.e. cancel the query), then return code will be 2, otherwise 0
echo $?
