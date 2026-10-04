#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast build has no Brotli and Snappy, and several outcomes below depend on them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The server negotiates the response compression from the `Accept-Encoding` request header
# (`chooseHTTPCompressionMethod`) when `enable_http_compression` is set. This pins the negotiation:
# server-side preference order, `q`-values (including `q=0` rejections and the default `q=1`),
# the `*` wildcard, case-insensitive tokens, and that a token must match exactly (no substrings).

URL="${CLICKHOUSE_URL}&enable_http_compression=1&query=SELECT+%27Hello%2C+world%27"

function check()
{
    local encoding
    encoding=$(${CLICKHOUSE_CURL} -sS -D- -o /dev/null -H "Accept-Encoding: $1" "${URL}" | grep -i '^Content-Encoding:' | tr -d '\r' | awk '{print $2}')
    echo "[$1] -> ${encoding:-(none)}"
}

echo "--- server preference order ---"
check "zstd, gzip, br"
check "br, gzip"
check "gzip"

echo "--- q=0 rejects an encoding ---"
check "br;q=0, gzip"
check "zstd;q=0"
check "gzip;q=0.0"
check "zstd;q=0, br;q=0.000"

echo "--- missing q defaults to 1 ---"
check "gzip;q=0.5, zstd"
check "zstd;q=0.1, br;q=1"
check "br;q=0.8, gzip"
check "bz2;q=0.8, gzip"

echo "--- no substring false positives ---"
check "X-gzip-custom"
check "my-br-extra, gzip"

echo "--- whitespace is ignored ---"
check "  gzip, zstd  "
check "zstd; q=0, gzip ; q=1"

echo "--- tokens are case-insensitive ---"
check "GZip"
check "gZIP"
check "ZSTD"
check "zstd, GZIP"
check "Snappy;q=0.5, gzIP;q=0.3"
check "zstd;Q=0, GZip;Q=0.5"

echo "--- the * wildcard ---"
check "*"
check "*;q=0"
check "gzip;q=0, *;q=1"
check "zstd;q=0, *;q=1"

echo "--- empty Accept-Encoding ---"
encoding=$(${CLICKHOUSE_CURL} -sS -D- -o /dev/null -H "Accept-Encoding;" "${URL}" | grep -i '^Content-Encoding:' | tr -d '\r' | awk '{print $2}')
echo "[] -> ${encoding:-(none)}"

echo "--- negotiated bodies really use the advertised encoding ---"
${CLICKHOUSE_CURL} -sS -H "Accept-Encoding: zstd" "${URL}" | zstd -d
${CLICKHOUSE_CURL} -sS -H "Accept-Encoding: gzip;q=0.5, br;q=0" "${URL}" | gzip -d
