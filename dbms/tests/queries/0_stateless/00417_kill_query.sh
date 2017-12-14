#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

QUERY_FIELND_NUM=4

clickhouse-client --max_block_size=1 -q "SELECT sleep(1) FROM system.numbers LIMIT 4" &>/dev/null &
sleep 1
clickhouse-client -q "KILL QUERY WHERE query LIKE 'SELECT sleep(%' AND (elapsed >= 0.) SYNC" | cut -f $QUERY_FIELND_NUM

clickhouse-client --max_block_size=1 -q "SELECT sleep(1) FROM system.numbers LIMIT 5" &>/dev/null &
sleep 1
clickhouse-client -q "KILL QUERY WHERE query = 'SELECT sleep(1) FROM system.numbers LIMIT 5' ASYNC" | cut -f $QUERY_FIELND_NUM

clickhouse-client -q "KILL QUERY WHERE 0 ASYNC"
clickhouse-client -q "KILL QUERY WHERE 0 FORMAT TabSeparated"
clickhouse-client -q "KILL QUERY WHERE 0 SYNC FORMAT TabSeparated"
clickhouse-client -q "KILL QUERY WHERE 1 TEST" &>/dev/null
