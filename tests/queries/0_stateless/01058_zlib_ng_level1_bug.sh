#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

for i in $(seq 34530 1 34630); do ${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "${CLICKHOUSE_URL}&enable_http_compression=1&http_zlib_compression_level=1" -d "SELECT * FROM numbers($i)" | gzip -d | tail -n1; done
