#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that snappy compression works in HTTP responses (snappy framing format).

# Verify Content-Encoding header is set.
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: snappy' -d 'SELECT 1' 2>&1 | grep --text '< Content-Encoding: snappy'

# Verify the response starts with the snappy framing format stream identifier.
# The 10-byte identifier is: 0xff 0x06 0x00 0x00 "sNaPpY"
RESPONSE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: snappy' \
    -d 'SELECT number FROM system.numbers LIMIT 5' | od -A n -t x1 -N 10 | tr -d ' \n')

EXPECTED="ff060000734e61507059"  # full 10-byte stream identifier: ff 06 00 00 "sNaPpY"
ACTUAL="${RESPONSE:0:20}"

if [ "$ACTUAL" = "$EXPECTED" ]; then
    echo "OK: snappy framing stream identifier found"
else
    echo "FAIL: expected stream identifier starting with $EXPECTED, got $ACTUAL" >&2
    exit 1
fi

# Verify request decoding (`Content-Encoding: snappy` on POST body).
# Round-trip: ask the server to encode a query string into framed snappy via
# `Accept-Encoding: snappy`, then POST that framed body back as the request body.
# The server must decode it through `SnappyFramedReadBuffer` and run "SELECT 99".
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" \
    -H 'Accept-Encoding: snappy' \
    -d "SELECT 'SELECT 99' FORMAT RawBLOB" \
    | ${CLICKHOUSE_CURL} -sS --data-binary @- \
        -H 'Content-Encoding: snappy' \
        "${CLICKHOUSE_URL}"

# Verify that a malformed framed-snappy request body is rejected with a decode
# exception (rather than silently accepted). Send only the truncated stream
# identifier prefix `\xff\x06\x00\x00\x73\x4e\x61` (7 of 10 bytes) and ensure
# the response is an error mentioning truncated/invalid snappy stream.
if printf '\xff\x06\x00\x00\x73\x4e\x61' | ${CLICKHOUSE_CURL} -sS --data-binary @- \
        -H 'Content-Encoding: snappy' \
        "${CLICKHOUSE_URL}" 2>&1 \
    | grep -qE 'Truncated snappy stream|Invalid snappy framing format|SNAPPY_UNCOMPRESS_FAILED'
then
    echo "OK: malformed snappy request rejected"
else
    echo "FAIL: malformed snappy request was not rejected" >&2
    exit 1
fi
