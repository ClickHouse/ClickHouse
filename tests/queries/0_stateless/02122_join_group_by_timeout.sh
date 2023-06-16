#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

MAX_PROCESS_WAIT=5

# TCP CLIENT: As of today (02/12/21) uses PullingAsyncPipelineExecutor
### Should be cancelled after 1 second and return a 159 exception (timeout)
timeout -s KILL $MAX_PROCESS_WAIT $CLICKHOUSE_CLIENT --max_execution_time 1 -q \
    "SELECT * FROM
    (
        SELECT a.name as n
        FROM
        (
            SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
        ) AS a,
        (
            SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
        ) as b
        GROUP BY n
    )
    LIMIT 20
    FORMAT Null" 2>&1 | grep -o "Code: 159" | sort | uniq

### Should stop pulling data and return what has been generated already (return code 0)
timeout -s KILL $MAX_PROCESS_WAIT $CLICKHOUSE_CLIENT -q \
    "SELECT a.name as n
     FROM
     (
         SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
     ) AS a,
     (
         SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
     ) as b
     FORMAT Null
     SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'
    "
echo $?


# HTTP CLIENT: As of today (02/12/21) uses PullingPipelineExecutor
### Should be cancelled after 1 second and return a 159 exception (timeout)
${CLICKHOUSE_CURL} -q --max-time $MAX_PROCESS_WAIT -sS "$CLICKHOUSE_URL&max_execution_time=1" -d \
    "SELECT * FROM
    (
        SELECT a.name as n
        FROM
        (
            SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
        ) AS a,
        (
            SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
        ) as b
        GROUP BY n
    )
    LIMIT 20
    FORMAT Null" 2>&1 | grep -o "Code: 159" | sort | uniq


### Should stop pulling data and return what has been generated already (return code 0)
${CLICKHOUSE_CURL} -q --max-time $MAX_PROCESS_WAIT -sS "$CLICKHOUSE_URL" -d \
    "SELECT a.name as n
          FROM
          (
              SELECT 'Name' as name, number FROM system.numbers LIMIT 2000000
          ) AS a,
          (
              SELECT 'Name' as name2, number FROM system.numbers LIMIT 2000000
          ) as b
          FORMAT Null
          SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'
    "
echo $?
