#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The structure-mismatch explanation must not be attached when the failing value is a value-level error
# and the rest of the row is valid input for the parser: rows the parser never reached, valid
# conversions and valid binary values of the sibling columns must not count as a mismatch.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

run() {
    printf "$1" | $CLICKHOUSE_LOCAL $4 --query "CREATE TABLE t ($2) ENGINE = Memory; INSERT INTO t FORMAT $3" 2>&1 | check
}

echo "-- CSV: the header detection must not sample the unreached second row"
run '"1",x\n"1",x,extra\n' 'a UInt8, b DateTime' CSV
run '"1",x\n' 'a UInt8, b DateTime' CSV

echo "-- TSVWithNames with parallel parsing: the header row is not a data row"
run 'a\tb\n1\t2\n1\t2\n1\t1.5\n2\ttext\n' 'a UInt8, b UInt8' TSVWithNames \
    '--input_format_parallel_parsing 1 --max_insert_block_size 3 --min_chunk_bytes_for_parallel_parsing 1 --max_threads 4'
run 'a\tb\n1\t2\n1\t2\ntext\t1\n2\ttext\n' 'a UInt8, b UInt8' TSVWithNames \
    '--input_format_parallel_parsing 1 --max_insert_block_size 3 --min_chunk_bytes_for_parallel_parsing 1 --max_threads 4'

echo "-- Values: a query parameter is read with its declared type"
$CLICKHOUSE_LOCAL --param_x=1 --query "CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t VALUES ({x:UInt8}, 'oops')" < /dev/null 2>&1 | check
$CLICKHOUSE_LOCAL --param_x=1 --query "CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t VALUES ({x:UInt8}, 1.5)" < /dev/null 2>&1 | check

echo "-- TSV / CSV: numbers that schema inference keeps as String"
run '01\t1.5\n' 'a UInt8, b UInt8' TSV
run '1\t1.5\n' 'a UInt8, b UInt8' TSV '--input_format_tsv_use_best_effort_in_schema_inference 0'
run '1,1.5\n' 'a UInt8, b UInt8' CSV '--input_format_csv_use_best_effort_in_schema_inference 0'
run 'x\t1.5\n' 'a UInt8, b UInt8' TSV '--input_format_tsv_use_best_effort_in_schema_inference 0'

echo "-- Native: valid scalar conversions beside a failing UUID"
native() {
    $CLICKHOUSE_LOCAL --query "SELECT $1 AS d, 'not-a-uuid' AS u FORMAT Native" \
        | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (d $2, u UUID) ENGINE = Memory; INSERT INTO t FORMAT Native" 2>&1 | check
}
native "toDate32('1970-01-02')" UInt8
native "toDecimal64(1.5, 2)" Float64
native "toFixedString('1', 1)" UInt8

echo "-- BSONEachRow: a 16-byte binary value into Int128 beside a failing UUID"
run '\x29\x00\x00\x00\x05x\x00\x10\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04abcd\x00' \
    'x Int128, u UUID' BSONEachRow
echo "-- BSONEachRow: a 3-byte binary value into Int128"
run '\x1b\x00\x00\x00\x05x\x00\x03\x00\x00\x00\x00abc\x02u\x00\x04\x00\x00\x00abc\x00\x00' \
    'x Int128, u UInt8' BSONEachRow
