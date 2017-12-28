#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

TABLE_HASH="cityHash64(groupArray(cityHash64(*)))"

function pack_unpack_compare()
{
    local buf_file="${CLICKHOUSE_TMP}/test.buf.'.$3"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.buf"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.buf_file"

    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.buf ENGINE = Memory AS $1"
    local res_orig=$(${CLICKHOUSE_CLIENT} --max_threads=1 --query "SELECT $TABLE_HASH FROM test.buf")

    ${CLICKHOUSE_CLIENT} --max_threads=1 --query "CREATE TABLE test.buf_file ENGINE = File($3) AS SELECT * FROM test.buf"
    local res_db_file=$(${CLICKHOUSE_CLIENT} --max_threads=1 --query "SELECT $TABLE_HASH FROM test.buf_file")

    ${CLICKHOUSE_CLIENT} --max_threads=1 --query "SELECT * FROM test.buf FORMAT $3" > "$buf_file"
    local res_ch_local1=$(${CLICKHOUSE_LOCAL} --structure "$2" --file "$buf_file" --table "my super table" --input-format "$3" --output-format TabSeparated --query "SELECT $TABLE_HASH FROM \`my super table\`" 2>${CLICKHOUSE_TMP}/stderr || cat stderr 1>&2)
    local res_ch_local2=$(${CLICKHOUSE_LOCAL} --structure "$2" --table "my super table" --input-format "$3" --output-format TabSeparated --query "SELECT $TABLE_HASH FROM \`my super table\`" < "$buf_file" 2>${CLICKHOUSE_TMP}/stderr || cat ${CLICKHOUSE_TMP}/stderr 1>&2)

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.buf"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.buf_file"
    rm -f "${CLICKHOUSE_TMP}/$buf_file" stderr

    echo $((res_orig - res_db_file)) $((res_orig - res_ch_local1)) $((res_orig - res_ch_local2))
}

pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number UInt64" "TabSeparated"
pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number UInt64" "Native"
pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number UInt64" "JSONEachRow"
echo
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name String, is_aggregate UInt8" "TabSeparated"
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name String, is_aggregate UInt8" "Native"
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name String, is_aggregate UInt8" "TSKV"
echo
${CLICKHOUSE_LOCAL} -q "CREATE TABLE sophisticated_default
(
    a UInt8 DEFAULT
    (
        SELECT number FROM system.numbers LIMIT 3,1
    ),
    b UInt8 ALIAS
    (
        SELECT dummy+9 FROM system.one
    ),
    c UInt8
) ENGINE = Memory; SELECT count() FROM system.tables WHERE name='sophisticated_default';" 2>/dev/null
