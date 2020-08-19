#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="SELECT 6103"

for (( i=1; i <= 50; i++ )) 
do

${CLICKHOUSE_CLIENT} --query="SYSTEM FLUSH LOGS"
sleep 0.1;
if [[ $($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT count() > 0 FROM system.text_log WHERE position(system.text_log.message, 'SELECT 6103') > 0 AND event_date >= yesterday()") == 1 ]]; then echo 1; exit; fi;

done;


