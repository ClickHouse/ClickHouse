#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: MsgPack format is not supported in fast test

# Regression test: MsgPack schema inference built the whole object tree with msgpack::unpack, which
# eagerly allocates storage for every array/map/str/bin sized by the count/length declared in the
# header, before reading the payload. A tiny corrupted or fuzzed header (e.g. an array32/map32/str32
# declaring 0xffffffff elements) drove a single multi-gigabyte allocation that bypassed the query
# memory tracker and aborted under a sanitizer (allocation-size-too-big) or threw std::bad_alloc.
# Each container must now be bounded by the bytes actually available, so an over-declaration is
# rejected cleanly (never a huge allocation), while valid data still infers normally.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Reads a query's combined output from stdin. If it contains $2, print a stable "<label>: <marker>"
# line; otherwise print the actual output so a CI failure shows what really happened instead of FAIL.
expect_contains() {
    local label="$1" marker="$2" out
    out=$(cat)
    if printf '%s\n' "$out" | grep -qF "$marker"; then
        echo "$label: $marker"
    else
        echo "$label: expected '$marker', got:"
        printf '%s\n' "$out" | head -3
    fi
}

# The four msgpack headers whose declared size drives an eager allocation. Each is a 5-byte header
# declaring 0xffffffff elements/bytes with no payload: rejected as UNEXPECTED_END_OF_FILE, never a
# multi-gigabyte allocation.
#   0xdd = array32, 0xdf = map32, 0xdb = str32, 0xc6 = bin32
for header_label in "array32:\xdd" "map32:\xdf" "str32:\xdb" "bin32:\xc6"; do
    label="${header_label%%:*}"
    byte="${header_label#*:}"
    printf "${byte}\xff\xff\xff\xff" \
        | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 \
            -q "SELECT toTypeName(c1) FROM table" 2>&1 \
        | expect_contains "$label" UNEXPECTED_END_OF_FILE
done

# A map is bounded by pairs, not bytes: msgpack allocates sizeof(object_kv) (key + value) per declared
# pair. A valid pair is two encoded objects (>= 2 bytes), so the map slot is capped at available / 2
# rather than available. This map32 buffers a full payload of n single-byte objects, so it passes the
# old raw-available map check (n <= 5 + n) and would allocate n * sizeof(object_kv) before discovering
# the payload holds at most n / 2 pairs; the tightened available / 2 bound rejects it before that
# allocation. Output is a clean rejection either way; the case guards the tighter bound stays correct.
python3 -c "import sys,struct; n=100000; sys.stdout.buffer.write(b'\xdf'+struct.pack('>I',n)+b'\x01'*n)" \
    | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 \
        -q "SELECT toTypeName(c1) FROM table" 2>&1 \
    | expect_contains "map_underbound" UNEXPECTED_END_OF_FILE

# The tightened map bound must not reject a valid map at the boundary. A fixmap of 15 pairs of
# single-byte objects has available = 1 + 2*15 = 31, so available / 2 = 15 == the pair count: the
# tightest valid map still passes and infers normally.
python3 -c "import sys; sys.stdout.buffer.write(b'\x8f' + b'\x01\x02'*15)" \
    | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 \
        -q "SELECT toTypeName(c1) FROM table"

# Valid data must still be inferred correctly (the bound must not reject legitimate objects). In
# particular a real string whose length header far exceeds the first buffered chunk must be read by
# growing the buffer, not rejected.
printf '\x2a' \
    | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 \
        -q "SELECT toTypeName(c1), c1 FROM table"

printf '\x93\x01\x02\x03' \
    | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 \
        -q "SELECT toTypeName(c1), c1 FROM table"

# A valid str32 with a 2 MB payload: the declared length is far larger than the initial buffered
# chunk, so schema inference must grow the buffer and accept it (must NOT be rejected as an
# over-declaration).
python3 -c "import sys,struct; n=2*1024*1024; sys.stdout.buffer.write(b'\xdb'+struct.pack('>I',n)+b'z'*n)" \
    | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 \
        -q "SELECT toTypeName(c1), length(c1) FROM table"

# Nesting depth is bounded too: max_parser_depth is threaded into msgpack::unpack, so a deeply nested
# header is rejected while the DOM tree is being built (before getDataType walks it). 0x91 is a fixarray
# of length 1, so a chain of them nests one level per byte. max_parser_depth is set above the SQL
# parser's own needs so that only the msgpack depth limit fires: a chain deeper than the limit is
# rejected as TOO_DEEP_RECURSION.
python3 -c "import sys; sys.stdout.buffer.write(b'\x91'*100 + b'\x01')" \
    | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 --max_parser_depth=42 \
        -q "SELECT toTypeName(c1) FROM table" 2>&1 \
    | expect_contains "deep" TOO_DEEP_RECURSION

# A chain within the limit still infers normally (the bound must not reject legitimate nesting).
python3 -c "import sys; sys.stdout.buffer.write(b'\x91'*10 + b'\x01')" \
    | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 --max_parser_depth=42 \
        -q "SELECT toTypeName(c1) FROM table"
