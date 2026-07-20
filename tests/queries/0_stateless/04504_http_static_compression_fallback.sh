#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: curl-based web-UI static-handler test, matches sibling 04502

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

echo "--- test 1: unsupported Accept-Encoding (identity) -> no Content-Encoding ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: identity" "${BASE_URL}/play" | grep -oF 'Content-Encoding:' || echo "no Content-Encoding (expected)"

echo "--- test 2: unsupported Accept-Encoding still advertises Vary ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: identity" "${BASE_URL}/play" | grep -oF 'Vary: Accept-Encoding'

echo "--- test 3: unknown coding (compress) -> no Content-Encoding ---"
${CLICKHOUSE_CURL} -sS -I -H "Accept-Encoding: compress" "${BASE_URL}/play" | grep -oF 'Content-Encoding:' || echo "no Content-Encoding (expected)"

echo "--- test 4: body is valid uncompressed HTML ---"
${CLICKHOUSE_CURL} -sS -H "Accept-Encoding: identity" "${BASE_URL}/play" | grep -o -F 'clickhouse.com'
