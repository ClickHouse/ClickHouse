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
