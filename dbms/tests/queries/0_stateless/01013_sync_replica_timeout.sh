#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE table_1013_1 (x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/${CLICKHOUSE_DATABASE}.table_1013', 'r1') ORDER BY x;
    CREATE TABLE table_1013_2 (x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/${CLICKHOUSE_DATABASE}.table_1013', 'r2') ORDER BY x;

    SYSTEM STOP FETCHES table_1013_2;
    INSERT INTO table_1013_1 VALUES (1)
"

timeout 2s ${CLICKHOUSE_CLIENT} -n -q "SET receive_timeout=1; SYSTEM SYNC REPLICA table_1013_2" 2>&1 \
| fgrep -q "DB::Exception: SYNC REPLICA ${CLICKHOUSE_DATABASE}.table_1013_2: command timed out!" \
    && echo 'OK' \
    || (${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query = 'SYSTEM SYNC REPLICA table_1013_2'"; echo "Failed!")

${CLICKHOUSE_CLIENT} -n -q "
    DROP TABLE IF EXISTS table_1013_2;
    DROP TABLE IF EXISTS table_1013_1;"
