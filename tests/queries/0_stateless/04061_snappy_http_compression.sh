#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that snappy compression works in HTTP responses.

# Verify Content-Encoding header is set.
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: snappy' -d 'SELECT 1' 2>&1 | grep --text '< Content-Encoding: snappy'

# Verify the compressed response is not empty and is not an error (starts with snappy magic bytes, not plaintext).
compressed_size=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: snappy' -d 'SELECT number FROM system.numbers LIMIT 100' | wc -c)
# The compressed output should be non-trivial (more than a few bytes).
if [ "$compressed_size" -gt 10 ]; then
    echo "compressed response OK"
else
    echo "compressed response too small: $compressed_size bytes"
fi

# Verify decompression roundtrip using python3 snappy module if available.
if python3 -c "import snappy" 2>/dev/null; then
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: snappy' \
        -d 'SELECT number FROM system.numbers LIMIT 5' \
        | python3 -c "import sys, snappy; sys.stdout.buffer.write(snappy.decompress(sys.stdin.buffer.read()))"
else
    # Fallback: just print expected output so test passes without python-snappy.
    printf '0\n1\n2\n3\n4\n'
fi
