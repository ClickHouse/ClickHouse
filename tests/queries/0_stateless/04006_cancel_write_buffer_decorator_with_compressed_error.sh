#!/usr/bin/env bash
# Regression test for WriteBufferDecorator::cancelImpl writing to a finalized buffer.
#
# When an HTTP request with response compression (Accept-Encoding: lz4/gzip/...)
# fails during query analysis (before any data is sent), cancelWithException
# finalizes the HTTP response buffer but leaves intermediate compression buffers
# un-finalized. The subsequent Output::cancel then calls cancel on those buffers,
# and WriteBufferDecorator::cancelImpl must not attempt to flush data to the
# already-finalized downstream buffer.
#
# In debug builds this caused: Logical error 'Cannot write to finalized buffer'.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Send queries that fail during analysis (type errors) via HTTP with various
# compression encodings. The server must return an error without crashing.
for encoding in lz4 gzip zstd br deflate; do
    # Use -o /dev/null to discard compressed body, check HTTP status code
    http_code=$(${CLICKHOUSE_CURL} -sS -o /dev/null -w "%{http_code}" \
        "${CLICKHOUSE_URL}&enable_http_compression=1" \
        -H "Accept-Encoding: $encoding" \
        -d "SELECT negate('not a number')")

    # Query analysis errors return HTTP 400
    if [ "$http_code" -ge 400 ] 2>/dev/null; then
        echo "$encoding: error returned"
    else
        echo "$encoding: unexpected http_code=$http_code"
    fi
done

# Verify server is still alive after all the error queries with compression
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT 'alive'"
