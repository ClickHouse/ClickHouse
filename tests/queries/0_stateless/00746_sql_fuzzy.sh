#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

export SQL_FUZZY_FILE_FUNCTIONS=${CLICKHOUSE_TMP}/clickhouse-functions
$CLICKHOUSE_CLIENT -q "select name from system.functions format TSV;" > "$SQL_FUZZY_FILE_FUNCTIONS"

export SQL_FUZZY_FILE_TABLE_FUNCTIONS=${CLICKHOUSE_TMP}/clickhouse-table_functions
$CLICKHOUSE_CLIENT -q "select name from system.table_functions format TSV;" > "$SQL_FUZZY_FILE_TABLE_FUNCTIONS"

# This is short run for ordinary tests.
# if you want long run use: env SQL_FUZZY_RUNS=100000 clickhouse-test sql_fuzzy

for SQL_FUZZY_RUN in $(seq "${SQL_FUZZY_RUNS:=10}"); do
    env SQL_FUZZY_RUN="$SQL_FUZZY_RUN" "$CURDIR"/00746_sql_fuzzy.pl | $CLICKHOUSE_CLIENT --format Null --max_execution_time 10 -n --ignore-error >/dev/null 2>&1
    if [[ $($CLICKHOUSE_CLIENT -q "SELECT 'Still alive'") != 'Still alive' ]]; then
        break
    fi
done

$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'"

# Query replay:
# cat clickhouse-server.log  | grep -aF "<Debug> executeQuery: (from " | perl -lpe 's/^.*executeQuery: \(from \S+\) (.*)/$1;/' | clickhouse-client -n --ignore-error
