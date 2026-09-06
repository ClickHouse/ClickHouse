#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Schema inference for the -WithNamesAndTypes formats returns the types declared in the second header row
# verbatim, but with `input_format_with_types_use_header = 0` the parser ignores that row and reads the
# data by value instead. The schema-mismatch explanation must not be derived from a types row the parser
# ignores, otherwise it lands on an unrelated value-level parse error in another column. The same applies
# to a types row auto-detected in the data of a plain format (`*_detect_header`).

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- CSVWithNamesAndTypes, input_format_with_types_use_header=0: the ignored types row must not be reported"
printf 'CREATE TABLE t (x UInt8, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSVWithNamesAndTypes\n"x","y"\n"String","UInt8"\n"1",1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_with_types_use_header 0 2>&1 | check

echo "-- CSVWithNamesAndTypes, input_format_with_types_use_header=1 (default): the parser checks the declared types, so the mismatch is reported"
printf 'CREATE TABLE t (x UInt8, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSVWithNamesAndTypes\n"x","y"\n"String","UInt8"\n"1",1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_with_types_use_header 1 2>&1 | check

echo "-- CSV with an auto-detected types row, input_format_with_types_use_header=0: the ignored types row must not be reported"
printf 'CREATE TABLE t (x UInt8, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV\n"x","y"\n"String","UInt8"\n"1",1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_csv_detect_header 1 --input_format_with_types_use_header 0 2>&1 | check

echo "-- CSV with an auto-detected types row, input_format_with_types_use_header=1 (default): the mismatch is reported"
printf 'CREATE TABLE t (x UInt8, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV\n"x","y"\n"String","UInt8"\n"1",1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_csv_detect_header 1 --input_format_with_types_use_header 1 2>&1 | check

echo "-- CSV without any header row: the types are inferred from the data, so a genuine mismatch is still reported"
printf 'CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV\ntext\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
