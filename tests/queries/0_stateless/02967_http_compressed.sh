#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

curl -sS -d 'SELECT 1,2,3,42' -H 'Accept-Encoding: gzip' -X POST "${CLICKHOUSE_URL}&compress=1&enable_http_compression=1" | gzip -d | hexdump
curl -sS -d 'SELECT 1,2,3,42' -H 'Accept-Encoding: br' -X POST "${CLICKHOUSE_URL}&compress=1&enable_http_compression=1" | brotli -d | hexdump
curl -sS -d 'SELECT 1,2,3,42' -H 'Accept-Encoding: xz' -X POST "${CLICKHOUSE_URL}&compress=1&enable_http_compression=1" | xz -d | hexdump
curl -sS -d 'SELECT 1,2,3,42' -H 'Accept-Encoding: zstd' -X POST "${CLICKHOUSE_URL}&compress=1&enable_http_compression=1" | zstd -d | hexdump
curl -sS -d 'SELECT 1,2,3,42' -H 'Accept-Encoding: bz2' -X POST "${CLICKHOUSE_URL}&compress=1&enable_http_compression=1" | bzip2 -d | hexdump
