#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_test_02585_query_to_kill_id_1"

$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" --max_rows_to_read 0 -q "
create temporary table tmp as select * from numbers(100000000);
select * from remote('127.0.0.2', 'system.numbers_mt') where number in (select * from tmp);" &> /dev/null &

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

while true
do
    res=$($CLICKHOUSE_CLIENT -q "select query, event_time from system.query_log where query_id = '$QUERY_ID' and current_database = '$CLICKHOUSE_DATABASE' and query like 'select%' limit 1")
    if [ -n "$res" ]; then
        break
    fi
    sleep 1
done

$CLICKHOUSE_CLIENT -q "kill query where query_id = '$QUERY_ID' sync" &> /dev/null
