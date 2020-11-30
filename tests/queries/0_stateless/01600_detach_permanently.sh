#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

## tests with real clickhouse restart would be a bit to heavy,
## to ensure the table will not reappear back clickhose-local is enough.

# TODO: clean it...
# CLICKHOUSE_LOCAL="/home/mfilimonov/workspace/ClickHouse-detach-permanently/build/programs/clickhouse-local"
# CLICKHOUSE_TMP=$(pwd)

WORKING_FOLDER_01600="${CLICKHOUSE_TMP}/01600_detach_permanently"
rm -rf "${WORKING_FOLDER_01600}"
mkdir -p "${WORKING_FOLDER_01600}"

clickhouse_local() {
    local query="$1"
    shift
    ${CLICKHOUSE_LOCAL} --query "$query" $@ -- --path="${WORKING_FOLDER_01600}"
}

test_detach_attach_sequence() {
    local db="$1"
    local table="$2"
    echo "#####"

    echo "${db}.${table} 1"
    # normal DETACH - while process is running (clickhouse-local here, same for server) table is detached.
    clickhouse_local "DETACH TABLE ${db}.${table}; SELECT if( count() = 0, '>table detached!', '>Fail') FROM system.tables WHERE database='${db}' AND name='${table}';"

    # but once we restart the precess (either clickhouse-local either clickhouse server) the table is back.
    echo "${db}.${table} 2"
    clickhouse_local "SELECT if(name='${table}', '>Table is back after restart', '>fail') FROM system.tables WHERE database='${db}' AND name='${table}'; SELECT count() FROM ${db}.${table};"

    # permanent DETACH - table is detached, and metadata file renamed, prevening further attach
    echo "${db}.${table} 3"
    clickhouse_local "DETACH TABLE ${db}.${table} PERMANENTLY; SELECT if( count() = 0, '>table detached (permanently)!', '>Fail') FROM system.tables WHERE database='${db}' AND name='${table}';"

    # still detached after restart
    echo "${db}.${table} 4"
    clickhouse_local "SELECT if( count() = 0, '>table is still detached (after restart)!', '>Fail') FROM system.tables WHERE database='${db}' AND name='${table}';"

    # but can be reattached
    echo "${db}.${table} 5"
    clickhouse_local "ATTACH TABLE ${db}.${table}; SELECT if(name='${table}', '>Table is back after attach', '>fail') FROM system.tables WHERE database='${db}' AND name='${table}';"

    echo "${db}.${table} 6"
    clickhouse_local "SELECT count() FROM ${db}.${table};"
}

clickhouse_local "DROP DATABASE IF EXISTS db_ordinary SYNC;"
clickhouse_local "DROP DATABASE IF EXISTS db_atomic SYNC;"

clickhouse_local "CREATE DATABASE db_ordinary Engine=Ordinary"
clickhouse_local "CREATE DATABASE db_atomic Engine=Atomic"

clickhouse_local "CREATE TABLE db_ordinary.log_table Engine=Log AS SELECT * FROM numbers(10)"
clickhouse_local "CREATE TABLE db_ordinary.mt_table Engine=MergeTree ORDER BY tuple() AS SELECT * FROM numbers(10)"
clickhouse_local "CREATE TABLE db_ordinary.null_table Engine=Null AS SELECT * FROM numbers(10)"

clickhouse_local "CREATE TABLE db_atomic.log_table Engine=Log AS SELECT * FROM numbers(10)"
clickhouse_local "CREATE TABLE db_atomic.mt_table Engine=MergeTree ORDER BY tuple() AS SELECT * FROM numbers(10)"
clickhouse_local "CREATE TABLE db_atomic.null_table Engine=Null AS SELECT * FROM numbers(10)"

test_detach_attach_sequence "db_ordinary" "log_table"
test_detach_attach_sequence "db_ordinary" "mt_table"
test_detach_attach_sequence "db_ordinary" "null_table"

test_detach_attach_sequence "db_atomic" "log_table"
test_detach_attach_sequence "db_atomic" "mt_table"
test_detach_attach_sequence "db_atomic" "null_table"