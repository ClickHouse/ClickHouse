#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

export SQL_FUZZY_FILE_FUNCTIONS=${CLICKHOUSE_TMP}/clickhouse-functions
$CLICKHOUSE_CLIENT -q "select name from system.functions format TSV;" > $SQL_FUZZY_FILE_FUNCTIONS

export SQL_FUZZY_FILE_TABLE_FUNCTIONS=${CLICKHOUSE_TMP}/clickhouse-table_functions
$CLICKHOUSE_CLIENT -q "select name from system.table_functions format TSV;" > $SQL_FUZZY_FILE_TABLE_FUNCTIONS

# This is short run for ordinary tests.
# if you want long run use: env SQL_FUZZY_RUNS=100000 clickhouse-test sql_fuzzy

for i in $(seq ${SQL_FUZZY_RUNS:=100}); do
    $CURDIR/00746_sql_fuzzy.pl | $CLICKHOUSE_CLIENT -n --ignore-error >/dev/null 2>&1
done

$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'"
