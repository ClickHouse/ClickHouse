#!/bin/bash
set -e

QUERY_FIELND_NUM=4

# should be quite deterministic
clickhouse-client --max_block_size=1 -q "SELECT sleep(3) FROM system.numbers LIMIT 2" &>/dev/null &
clickhouse-client --max_block_size=1 -q "SELECT sleep(1) FROM system.numbers LIMIT 6" &>/dev/null &
clickhouse-client --max_block_size=1 -q "SELECT sleep(2) FROM system.numbers LIMIT 3" &>/dev/null &
clickhouse-client --max_block_size=1 -q "SELECT 'trash', sleep(2) FROM system.numbers LIMIT 3" &>/dev/null &
sleep 1 # wait initialization
clickhouse-client -q "KILL QUERY WHERE query LIKE 'SELECT sleep(%' AND (elapsed >= 0.) SYNC" | cut -f $QUERY_FIELND_NUM
clickhouse-client -q "SELECT countIf(query LIKE 'SELECT sleep(%') FROM system.processes"


clickhouse-client --max_block_size=1 -q "SELECT sleep(1) FROM system.numbers LIMIT 999" &>/dev/null &
sleep 1
clickhouse-client -q "KILL QUERY WHERE query = 'SELECT sleep(1) FROM system.numbers LIMIT 999' ASYNC" | cut -f $QUERY_FIELND_NUM
sleep 1 # wait cancelling
clickhouse-client -q "SELECT countIf(query = 'SELECT sleep(1) FROM system.numbers LIMIT 999') FROM system.processes"


clickhouse-client -q "KILL QUERY WHERE 0 ASYNC"
clickhouse-client -q "KILL QUERY WHERE 0 FORMAT TabSeparated"
clickhouse-client -q "KILL QUERY WHERE 0 SYNC FORMAT TabSeparated"
clickhouse-client -q "KILL QUERY WHERE 1 TEST" &>/dev/null
