#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

python3 -c "import snappy" 2>/dev/null || { echo "python3 snappy module not available"; exit 1; }

# Test that snappy compression works in HTTP responses (snappy framing format).

# Verify Content-Encoding header is set.
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: snappy' -d 'SELECT 1' 2>&1 | grep --text '< Content-Encoding: snappy'

# Verify the response can be decompressed (snappy framing format uses StreamDecompressor).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: snappy' \
    -d 'SELECT number FROM system.numbers LIMIT 5' \
    | python3 -c "
import sys
import snappy

data = sys.stdin.buffer.read()
decompressor = snappy.StreamDecompressor()
result = decompressor.decompress(data)
sys.stdout.buffer.write(result)
"
