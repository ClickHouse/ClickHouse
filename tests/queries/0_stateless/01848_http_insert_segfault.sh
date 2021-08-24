#!/usr/bin/env bash

 CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
 # shellcheck source=../shell_config.sh
 . "$CUR_DIR"/../shell_config.sh

 ${CLICKHOUSE_LOCAL} -q "select col1, initializeAggregation('argMaxState', col2, insertTime) as col2, now() as insertTime FROM generateRandom('col1 String, col2 Array(Float64)') LIMIT 1000000 FORMAT CSV"  | ${CLICKHOUSE_CURL} -s 'http://localhost:8123/?query=INSERT%20INTO%20non_existing_table%20SELECT%20col1%2C%20initializeAggregation(%27argMaxState%27%2C%20col2%2C%20insertTime)%20as%20col2%2C%20now()%20as%20insertTime%20FROM%20input(%27col1%20String%2C%20col2%20Array(Float64)%27)%20FORMAT%20CSV' --data-binary @- | grep -q "Table default.non_existing_table doesn't exist" && echo 'Ok.' || echo 'FAIL' ||:
