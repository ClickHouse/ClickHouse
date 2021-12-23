#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

## tests with real clickhouse restart would be a bit to heavy,
## to ensure the table will not reappear back clickhose-local is enough.

WORKING_FOLDER_01600="${CLICKHOUSE_TMP}/01600_detach_permanently"
rm -rf "${WORKING_FOLDER_01600}"
mkdir -p "${WORKING_FOLDER_01600}"

clickhouse_local() {
    local query="$1"
    shift
    ${CLICKHOUSE_LOCAL} --query "$query" "$@" --path="${WORKING_FOLDER_01600}"
}

test_detach_attach_sequence() {
    local db="$1"
    local table="$2"
    echo "##################"

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

echo "##################"
echo "setup env"

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

echo "##################"
echo "test for MV"
clickhouse_local "CREATE TABLE db_ordinary.src Engine=Null AS system.numbers"
clickhouse_local "CREATE TABLE db_ordinary.dst Engine=Log AS system.numbers"
clickhouse_local "CREATE MATERIALIZED VIEW db_ordinary.src2dst_mv_to TO db_ordinary.dst AS SELECT * FROM db_ordinary.src"

clickhouse_local "INSERT INTO db_ordinary.src SELECT * FROM numbers(10)"
clickhouse_local "SELECT if(count() = 10, 'MV is working', 'MV failed') from db_ordinary.dst;"

clickhouse_local "DETACH VIEW db_ordinary.src2dst_mv_to; INSERT INTO db_ordinary.src SELECT * FROM numbers(10)"
clickhouse_local "SELECT if(count() = 10, 'Usual detach works immediately till restart', 'Usual detach failed') from db_ordinary.dst;"

clickhouse_local "INSERT INTO db_ordinary.src SELECT * FROM numbers(10)"
clickhouse_local "SELECT if(count() = 20, 'Usual detach activates after restart', 'Usual detach reactivation failed') from db_ordinary.dst;"

clickhouse_local "DETACH VIEW db_ordinary.src2dst_mv_to PERMANENTLY; INSERT INTO db_ordinary.src SELECT * FROM numbers(10)"
clickhouse_local "SELECT if(count() = 20, 'Permanent detach works immediately', 'Permanent detach failed') from db_ordinary.dst;"

clickhouse_local "INSERT INTO db_ordinary.src SELECT * FROM numbers(10)"
clickhouse_local "SELECT if(count() = 20, 'Permanent detach still works after restart', 'Permanent detach reactivated!') from db_ordinary.dst;"

## Quite silly: ATTACH MATERIALIZED VIEW don't work with short syntax (w/o select), but i can attach it using ATTACH TABLE ...
clickhouse_local "ATTACH TABLE db_ordinary.src2dst_mv_to"
clickhouse_local "INSERT INTO db_ordinary.src SELECT * FROM numbers(10)"
clickhouse_local "SELECT if(count() = 30, 'View can be reattached', 'can not reattach permanently detached view') from db_ordinary.dst;"

clickhouse_local "DROP VIEW db_ordinary.src2dst_mv_to SYNC"

echo "##################"
echo "test for MV with inner table"
clickhouse_local "CREATE MATERIALIZED VIEW db_ordinary.src_mv_with_inner Engine=Log AS SELECT * FROM db_ordinary.src"
clickhouse_local "INSERT INTO db_ordinary.src SELECT * FROM numbers(10)"

clickhouse_local "SELECT if(count() = 10, 'MV is working', 'MV failed') FROM db_ordinary.src_mv_with_inner"

clickhouse_local "DETACH VIEW db_ordinary.src_mv_with_inner PERMANENTLY; INSERT INTO db_ordinary.src SELECT * FROM numbers(10)" --stacktrace
clickhouse_local "SELECT if(count() = 10, 'MV can be detached permanently', 'MV detach failed') FROM db_ordinary.src_mv_with_inner" 2>&1 | grep -c "db_ordinary.src_mv_with_inner doesn't exist"
## Quite silly: ATTACH MATERIALIZED VIEW don't work with short syntax (w/o select), but i can attach it using ATTACH TABLE ...
clickhouse_local "ATTACH TABLE db_ordinary.src_mv_with_inner"
clickhouse_local "INSERT INTO db_ordinary.src SELECT * FROM numbers(10)"
clickhouse_local "SELECT if(count() = 20, 'View can be reattached', 'can not reattach permanently detached view') from db_ordinary.src_mv_with_inner;"

## clickhouse_local can't work with dicts...
# mkdir -p "${WORKING_FOLDER_01600}/user_files"
# echo "1" > "${WORKING_FOLDER_01600}/user_files/dummy_dict.tsv"
# clickhouse_local "DROP DICTIONARY db_ordinary.dummy; CREATE DICTIONARY db_ordinary.dummy (id UInt64) PRIMARY KEY id LAYOUT(FLAT()) SOURCE(FILE(path 'dummy_dict.tsv' format 'TabSeparated')) LIFETIME(MIN 1 MAX 10); DETACH DICTIONARY db_ordinary.dummy PERMANENTLY; SELECT dictGet('db_ordinary.dummy','val',toUInt64(1));"

echo "##################"
echo "DETACH DATABASE is not implemented (proper error)"
clickhouse_local "DETACH DATABASE db_ordinary PERMANENTLY;" 2>&1 | grep -c 'DETACH PERMANENTLY is not implemented'

# clean up
rm -rf "${WORKING_FOLDER_01600}"
