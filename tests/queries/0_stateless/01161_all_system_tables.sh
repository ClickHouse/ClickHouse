#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: since someone may create table in system database

# Server may ignore some exceptions, but it still print exceptions to logs and (at least in CI) sends Error and Warning log messages to client
# making test fail because of non-empty stderr. Ignore such log messages.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

THREADS=8
RAND=$(($RANDOM))
LIMIT=10000

function run_selects()
{
    thread_num=$1
    readarray -t tables_arr < <(${CLICKHOUSE_CLIENT} -q "SELECT database || '.' || name FROM system.tables
    WHERE database in ('system', 'information_schema', 'INFORMATION_SCHEMA') and name != 'zookeeper' and name != 'models'
    AND sipHash64(name || toString($RAND)) % $THREADS = $thread_num AND name NOT LIKE '%\\_sender' AND name NOT LIKE '%\\_watcher' AND name != 'coverage_log'")

    for t in "${tables_arr[@]}"
    do
        ${CLICKHOUSE_CLIENT} -q "SELECT * FROM $t LIMIT $LIMIT SETTINGS allow_introspection_functions = 1 FORMAT Null" # Suppress style check: database=$CLICKHOUSE_DATABASEs
    done
}

for ((i=0; i<THREADS; i++)) do
    run_selects "$i" &
done
wait
