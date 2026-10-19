#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The internal `compress=1` output uses the codec from `network_compression_method`, whose default
# (`ZSTD` at `network_zstd_compression_level = 3`) matches the built-in default codec.
curl -sS -d 'SELECT 1,2,3,42' -H 'Accept-Encoding: gzip' -X POST "${CLICKHOUSE_URL}&compress=1&enable_http_compression=1" | gzip -d | hexdump
curl -sS -d 'SELECT 1,2,3,42' -H 'Accept-Encoding: zstd' -X POST "${CLICKHOUSE_URL}&compress=1&enable_http_compression=1" | zstd -d | hexdump
