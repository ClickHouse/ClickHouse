#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

T="rb_ntd_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $T"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $T (id UInt32, name Nullable(String)) ENGINE = MergeTree ORDER BY id"

# --- helpers to write VarUInt/String/UInt32 little-endian ---
emit_byte() { printf "\\$(printf '%03o' "$1")"; }
emit_varuint() { emit_byte "$1"; }
emit_str() {
    local s="$1"
    emit_byte "${#s}"
    printf '%s' "$s"
}
emit_u32() {
    emit_byte $(( $1 & 0xff ))
    emit_byte $(( ($1 >> 8) & 0xff ))
    emit_byte $(( ($1 >> 16) & 0xff ))
    emit_byte $(( ($1 >> 24) & 0xff ))
}
emit_i32() {
    # interpret arg as signed 32-bit; output little-endian two's complement
    local v=$1
    if [[ $v -lt 0 ]]; then v=$(( v + 4294967296 )); fi
    emit_u32 "$v"
}

hdr2() {
    emit_varuint 2
    emit_str 'id'
    emit_str 'name'
    emit_str 'UInt32'
    emit_str 'Nullable(String)'
}

hdr3() {
    emit_varuint 3
    emit_str 'id'
    emit_str 'name'
    emit_str 'extra'
    emit_str 'UInt32'
    emit_str 'Nullable(String)'
    emit_str 'Nullable(Int32)'
}

hdr_reordered() {
    emit_varuint 3
    emit_str 'extra'
    emit_str 'id'
    emit_str 'name'
    emit_str 'Nullable(Int32)'
    emit_str 'UInt32'
    emit_str 'Nullable(String)'
}

hdr_bogus() {
    emit_varuint 3
    emit_str 'id'
    emit_str 'name'
    emit_str 'bogus'
    emit_str 'UInt32'
    emit_str 'Nullable(String)'
    emit_str 'Int32'
}

# ---- Step A: 2-column header, 3 rows incl NULL in name ----
{
    hdr2
    # row 1: id=1, name='a'
    printf '\x00'; emit_u32 1
    printf '\x00\x00'; emit_str 'a'
    # row 2: id=2, name=NULL
    printf '\x00'; emit_u32 2
    printf '\x00\x01'
    # row 3: id=3, name='c'
    printf '\x00'; emit_u32 3
    printf '\x00\x00'; emit_str 'c'
} | $CLICKHOUSE_CLIENT --query "INSERT INTO $T FORMAT RowBinaryWithNamesAndTypesAndDefaults"

# ---- Step B: schema evolution ----
$CLICKHOUSE_CLIENT --query "ALTER TABLE $T ADD COLUMN extra Nullable(Int32) DEFAULT 42"

# ---- Step C: old 2-column header still works ----
{
    hdr2
    printf '\x00'; emit_u32 4
    printf '\x00\x00'; emit_str 'd'
    printf '\x00'; emit_u32 5
    printf '\x00\x01'
    printf '\x00'; emit_u32 6
    printf '\x00\x00'; emit_str 'f'
} | $CLICKHOUSE_CLIENT --query "INSERT INTO $T FORMAT RowBinaryWithNamesAndTypesAndDefaults"

# ---- Step D: 3-column header, mix per-cell markers ----
{
    hdr3
    # row 10: id=10, name='x', extra=123
    printf '\x00'; emit_u32 10
    printf '\x00\x00'; emit_str 'x'
    printf '\x00\x00'; emit_i32 123
    # row 11: id=11, name='y', extra marker=0x01 (default 42)
    printf '\x00'; emit_u32 11
    printf '\x00\x00'; emit_str 'y'
    printf '\x01'
    # row 12: id=12, name=NULL, extra=NULL
    printf '\x00'; emit_u32 12
    printf '\x00\x01'
    printf '\x00\x01'
    # row 13: id=13, name='w', extra=-7
    printf '\x00'; emit_u32 13
    printf '\x00\x00'; emit_str 'w'
    printf '\x00\x00'; emit_i32 -7
} | $CLICKHOUSE_CLIENT --query "INSERT INTO $T FORMAT RowBinaryWithNamesAndTypesAndDefaults"

# ---- Step E: reordered header (extra, id, name) ----
{
    hdr_reordered
    # row 20: extra=99, id=20, name='r'
    printf '\x00\x00'; emit_i32 99
    printf '\x00'; emit_u32 20
    printf '\x00\x00'; emit_str 'r'
} | $CLICKHOUSE_CLIENT --query "INSERT INTO $T FORMAT RowBinaryWithNamesAndTypesAndDefaults"

# ---- Step F: header has an extra column "bogus Int32" not in the table.
# Exercise both branches of the marker-aware skip:
#   row 30: bogus marker=0x01 (no value follows)
#   row 31: bogus marker=0x00 + 4-byte Int32 value 555
# Both rows must land in the table with id/name set and 'bogus' silently dropped.
{
    hdr_bogus
    # row 30: id=30, name='s', bogus marker=0x01
    printf '\x00'; emit_u32 30
    printf '\x00\x00'; emit_str 's'
    printf '\x01'
    # row 31: id=31, name='t', bogus marker=0x00 + Int32 value 555
    printf '\x00'; emit_u32 31
    printf '\x00\x00'; emit_str 't'
    printf '\x00'; emit_i32 555
} | $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=1 --query "INSERT INTO $T FORMAT RowBinaryWithNamesAndTypesAndDefaults"

# Same payload with the setting OFF — expect an error, normalized to just the error name.
{
    hdr_bogus
    printf '\x00'; emit_u32 32
    printf '\x00\x00'; emit_str 'u'
    printf '\x01'
} | $CLICKHOUSE_CLIENT --input_format_skip_unknown_fields=0 --query "INSERT INTO $T FORMAT RowBinaryWithNamesAndTypesAndDefaults" 2>&1 \
    | grep -oE 'INCORRECT_DATA' | head -1

$CLICKHOUSE_CLIENT --query "SELECT id, name, extra FROM $T ORDER BY id"
$CLICKHOUSE_CLIENT --query "DROP TABLE $T"
