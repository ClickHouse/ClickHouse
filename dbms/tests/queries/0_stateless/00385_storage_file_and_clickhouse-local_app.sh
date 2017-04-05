#!/usr/bin/env bash
set -e

TABLE_HASH="cityHash64(groupArray(cityHash64(*)))"

function pack_unpack_compare()
{
    local buf_file="test.buf.'.$3"

    clickhouse-client --query "DROP TABLE IF EXISTS test.buf"
    clickhouse-client --query "DROP TABLE IF EXISTS test.buf_file"

    clickhouse-client --query "CREATE TABLE test.buf ENGINE = Memory AS $1"
    local res_orig=$(clickhouse-client --max_threads=1 --query "SELECT $TABLE_HASH FROM test.buf")

    clickhouse-client --max_threads=1 --query "CREATE TABLE test.buf_file ENGINE = File($3) AS SELECT * FROM test.buf"
    local res_db_file=$(clickhouse-client --max_threads=1 --query "SELECT $TABLE_HASH FROM test.buf_file")

    clickhouse-client --max_threads=1 --query "SELECT * FROM test.buf FORMAT $3" > "$buf_file"
    local res_ch_local1=$(clickhouse-local --structure "$2" --file "$buf_file" --table "my super table" --input-format "$3" --output-format TabSeparated --query "SELECT $TABLE_HASH FROM \`my super table\`" 2>stderr || cat stderr 1>&2)
    local res_ch_local2=$(clickhouse-local --structure "$2" --table "my super table" --input-format "$3" --output-format TabSeparated --query "SELECT $TABLE_HASH FROM \`my super table\`" < "$buf_file" 2>stderr || cat stderr 1>&2)

    clickhouse-client --query "DROP TABLE IF EXISTS test.buf"
    clickhouse-client --query "DROP TABLE IF EXISTS test.buf_file"
    rm -f "$buf_file" stderr

    echo $((res_orig - res_db_file)) $((res_orig - res_ch_local1)) $((res_orig - res_ch_local2))
}

pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number UInt64" "TabSeparated"
pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number UInt64" "Native"
pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number UInt64" "JSONEachRow"
echo
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name String, is_aggregate UInt8" "TabSeparated"
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name String, is_aggregate UInt8" "Native"
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name String, is_aggregate UInt8" "TSKV"
