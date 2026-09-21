#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! command -v zstd &> /dev/null; then echo "zstd not found" 1>&2; exit 1; fi
if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

echo "--- test 1: no Accept-Encoding, no Content-Encoding ---"
${CLICKHOUSE_CURL} -sS -I "${BASE_URL}/play" | grep -oF 'Content-Encoding:' || echo "no Content-Encoding (expected)"

echo "--- test 2: Accept-Encoding: zstd on /play ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: zstd" "${BASE_URL}/play" | grep -oF 'Content-Encoding: zstd'

echo "--- test 3: Accept-Encoding: gzip on /dashboard ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: gzip" "${BASE_URL}/dashboard" | grep -oF 'Content-Encoding: gzip'

echo "--- test 4: Accept-Encoding: zstd on /js/uplot.js ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: zstd" "${BASE_URL}/js/uplot.js" | grep -oF 'Content-Encoding: zstd'

echo "--- test 5: Accept-Encoding: zstd on /binary ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: zstd" "${BASE_URL}/binary" | grep -oF 'Content-Encoding: zstd'

echo "--- test 6: Accept-Encoding: zstd on /merges ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: zstd" "${BASE_URL}/merges" | grep -oF 'Content-Encoding: zstd'

echo "--- test 7: Accept-Encoding: zstd on /schema ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: zstd" "${BASE_URL}/schema" | grep -oF 'Content-Encoding: zstd'

echo "--- test 8: zstd response is valid and decompressable ---"
${CLICKHOUSE_CURL} -sS -H "Accept-Encoding: zstd" "${BASE_URL}/play" | zstd -d | grep -o -F 'clickhouse.com'

echo "--- test 9: gzip response is valid and decompressable ---"
${CLICKHOUSE_CURL} -sS -H "Accept-Encoding: gzip" "${BASE_URL}/play" | gzip -d | grep -o -F 'clickhouse.com'

echo "--- test 10: ClickStack still pre-gzipped ---"
${CLICKHOUSE_CURL} -sS -I "${BASE_URL}/clickstack" | grep -oF 'Content-Encoding: gzip'

echo "--- test 11: Vary: Accept-Encoding advertised on negotiated response ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: zstd" "${BASE_URL}/play" | grep -oF 'Vary: Accept-Encoding'

echo "--- test 12: Vary: Accept-Encoding advertised even without Accept-Encoding ---"
${CLICKHOUSE_CURL} -sS -I "${BASE_URL}/play" | grep -oF 'Vary: Accept-Encoding'

echo "--- test 13: pre-encoded response does not advertise Vary: Accept-Encoding ---"
${CLICKHOUSE_CURL} -sS -I "${BASE_URL}/clickstack" | grep -oF 'Vary: Accept-Encoding' || echo "no Vary (expected)"

echo "--- test 14: Accept-Encoding: zstd on / with Mozilla UA ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: zstd" -H "User-Agent: Mozilla/5.0" "${BASE_URL}/" | grep -oF 'Content-Encoding: zstd'
