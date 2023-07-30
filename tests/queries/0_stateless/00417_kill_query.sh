#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_FIELND_NUM=4

$CLICKHOUSE_CLIENT --max_block_size=1 -q "SELECT sleep(1) FROM system.numbers LIMIT 30" &>/dev/null &
sleep 1
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE current_database = '${CLICKHOUSE_DATABASE}' and query LIKE 'SELECT sleep(%' AND (elapsed >= 0.) SYNC" | cut -f $QUERY_FIELND_NUM

# 31 is for the query to be different from the previous one 
$CLICKHOUSE_CLIENT --max_block_size=1 -q "SELECT sleep(1) FROM system.numbers LIMIT 31" &>/dev/null &
sleep 1
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE current_database = '${CLICKHOUSE_DATABASE}' and query = 'SELECT sleep(1) FROM system.numbers LIMIT 31' ASYNC" | cut -f $QUERY_FIELND_NUM

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE 0 ASYNC"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE 0 FORMAT TabSeparated"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE 0 SYNC FORMAT TabSeparated"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE 1 TEST" &>/dev/null

