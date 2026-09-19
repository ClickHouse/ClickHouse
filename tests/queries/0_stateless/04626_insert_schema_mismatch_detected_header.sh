#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A plain CSV / TSV insert may carry an auto-detected header (`input_format_csv_detect_header`,
# enabled by default): the parser then maps the columns by name, and a detected types row is
# validated against the destination exactly. The schema-mismatch explanation must follow that,
# not treat the format as positional and value-inferred.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- CSV, detected header with reordered names: parser maps by name, only a value fails (no false positive)"
printf 'CREATE TABLE t (a String, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV\nb,a\n1.5,x\n' \
    | $CLICKHOUSE_LOCAL --input_format_csv_detect_header 1 2>&1 | check

echo "-- CSV, detected header with matching order: only a value fails (no false positive)"
printf 'CREATE TABLE t (a String, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV\na,b\nx,1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_csv_detect_header 1 2>&1 | check

echo "-- TSV, detected types row differs from the destination (exact type check applies)"
printf 'CREATE TABLE t (a UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\na\nUInt16\n1\n' \
    | $CLICKHOUSE_LOCAL --input_format_tsv_detect_header 1 2>&1 | check

echo "-- TSV, detected types row matches the destination, only a value fails (no false positive)"
printf 'CREATE TABLE t (a UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\na\nUInt8\n1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_tsv_detect_header 1 2>&1 | check
