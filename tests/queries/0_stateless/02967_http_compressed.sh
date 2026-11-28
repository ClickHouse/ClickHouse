#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The internal `compress=1` output is a compressed block inside the HTTP content encoding. It is decompressed
# before printing, because the bytes of a compressed block depend on the codec and on the version of its
# library, while its content does not.
curl -sS -d 'SELECT 1,2,3,42' -H 'Accept-Encoding: gzip' -X POST "${CLICKHOUSE_URL}&compress=1&enable_http_compression=1" | gzip -d | ${CLICKHOUSE_COMPRESSOR} --decompress
curl -sS -d 'SELECT 1,2,3,42' -H 'Accept-Encoding: zstd' -X POST "${CLICKHOUSE_URL}&compress=1&enable_http_compression=1" | zstd -d | ${CLICKHOUSE_COMPRESSOR} --decompress
