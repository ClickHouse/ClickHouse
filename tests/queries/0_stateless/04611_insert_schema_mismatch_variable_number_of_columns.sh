#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Some positional formats legally accept a number of columns that differs from the destination: missing
# trailing columns are filled with defaults and/or extra columns are skipped. `JSONCompactColumns` does
# this always, and `TSV` / `CSV` / `CustomSeparated` / `JSONCompactEachRow` do it when the corresponding
# `*_allow_variable_number_of_columns` setting is enabled. For such formats a differing column count is not
# by itself a structure mismatch, so the schema-mismatch explanation must not be attached to an unrelated
# value-level parse error just because schema inference sees fewer columns than the destination has.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONCompactColumns: fewer columns than the destination (trailing columns default-filled), only a value fails (no false positive)"
printf 'CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT JSONCompactColumns [[1.5]]\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONCompactColumns: the value present is genuinely the wrong shape (a string for a numeric column), so the mismatch is still explained"
printf 'CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT JSONCompactColumns [["abc"]]\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- TSV, input_format_tsv_allow_variable_number_of_columns=1: fewer columns accepted, only a value fails (no false positive)"
printf 'CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_tsv_allow_variable_number_of_columns 1 2>&1 | check

echo "-- TSV, input_format_tsv_allow_variable_number_of_columns=0 (default): the parser requires the exact number of columns, so the differing structure is explained"
printf 'CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1.5\n' \
    | $CLICKHOUSE_LOCAL --input_format_tsv_allow_variable_number_of_columns 0 2>&1 | check
