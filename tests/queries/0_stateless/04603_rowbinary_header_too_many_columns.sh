#!/usr/bin/env bash
# Tests that a RowBinaryWithNames[AndTypes] header with a suspiciously large column
# count is rejected with TOO_LARGE_ARRAY_SIZE instead of amplifying into a huge
# allocation. See https://github.com/ClickHouse/clickhouse-private/issues/69219

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_rowbinary_header"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_rowbinary_header (x UInt64) ENGINE = Memory"

# LEB128 encoder.
varint() {
    python3 -c '
import sys
n = int(sys.argv[1])
out = bytearray()
while True:
    b = n & 0x7F
    n >>= 7
    if n:
        b |= 0x80
    out.append(b)
    if not n:
        break
sys.stdout.buffer.write(bytes(out))
' "$1"
}

# Negative control: a well-formed one-column header (name "x", type "UInt64") plus one
# 8-byte UInt64 row inserts fine.
{ varint 1; printf '\x01x\x06UInt64'; printf '\x01\x00\x00\x00\x00\x00\x00\x00'; } \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_rowbinary_header+FORMAT+RowBinaryWithNamesAndTypes" --data-binary @-
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_rowbinary_header"

# Attack: a header claiming far more columns than the ceiling (1'000'000).
# The tiny body must be rejected up-front, not amplified.
varint 2000000 \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_rowbinary_header+FORMAT+RowBinaryWithNamesAndTypes" --data-binary @- 2>&1 \
    | grep -o "TOO_LARGE_ARRAY_SIZE" | head -n1

# Same for the WithNames variant (the names header is a separate read path).
varint 2000000 \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_rowbinary_header+FORMAT+RowBinaryWithNames" --data-binary @- 2>&1 \
    | grep -o "TOO_LARGE_ARRAY_SIZE" | head -n1

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_rowbinary_header"
