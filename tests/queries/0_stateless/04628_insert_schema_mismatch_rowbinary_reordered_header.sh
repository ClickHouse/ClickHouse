#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `RowBinaryWithNamesAndTypes` (and `RowBinaryWithNamesAndTypesAndDefaults`) carry a names header, and
# the parser maps the columns to the destination by name when `input_format_with_names_use_header` is
# enabled (the default) — even though these formats do not advertise
# `FormatFactory::checkIfFormatSupportsSubsetOfColumns`. The schema-mismatch explanation must therefore
# compare the inferred structure against the destination by name for them too, so a reordered header is
# not mistaken for a structure mismatch.
#
# The payload below declares its header as `b String, a UInt8` while the destination is `(a UInt8, b
# String)` — a valid reorder that the parser maps by name. The `b` string value is truncated (its
# declared length exceeds the bytes present), which fails with a genuine parse error
# (`CANNOT_READ_ALL_DATA`) that triggers the diagnostic. The by-name comparison must find the structure
# to match and not append the misleading "structure mismatch" suffix to this value-level error.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- RowBinaryWithNamesAndTypes: a reordered header is not a structure mismatch"
{
    echo "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT RowBinaryWithNamesAndTypes"
    # header: 2 columns; names 'b','a'; types 'String','UInt8'
    printf '\x02\x01b\x01a\x06String\x05UInt8'
    # row (header order b, a): b = String with declared length 5 but only 2 bytes present -> parse error
    printf '\x05hi'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- RowBinaryWithNamesAndTypesAndDefaults: a reordered header is not a structure mismatch"
{
    echo "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT RowBinaryWithNamesAndTypesAndDefaults"
    # header: 2 columns; names 'b','a'; types 'String','UInt8'
    printf '\x02\x01b\x01a\x06String\x05UInt8'
    # row (header order b, a): default-marker byte 0 for b, then String length 5 but only 2 bytes present
    printf '\x00\x05hi'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- RowBinaryWithNamesAndTypes: a genuine type mismatch in the header is still reported"
{
    echo "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT RowBinaryWithNamesAndTypes"
    # header declares column 'a' as String (destination is UInt8) -> exact type mismatch, reported
    printf '\x02\x01a\x01b\x06String\x06String'
    # row: a = String length 5 but only 2 bytes present -> parse error triggers the diagnostic
    printf '\x05hi'
} | $CLICKHOUSE_LOCAL 2>&1 | check
