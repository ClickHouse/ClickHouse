#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 26.7 accepted `gzip`/`deflate` compression levels 10-12 (they are clamped to zlib's maximum of 9),
# so configurations written against 26.7 must keep working on the HTTP surface:
# `enable_http_compression=1` + `http_zlib_compression_level=10..12` must still round-trip.

QUERY='SELECT sum(cityHash64(number)) FROM numbers(100000)'

for level in 10 11 12
do
    # The response must actually be compressed with the requested encoding.
    ${CLICKHOUSE_CURL} -vsS -H 'Accept-Encoding: gzip'    "${CLICKHOUSE_URL}&enable_http_compression=1&http_zlib_compression_level=${level}" -d "${QUERY}" 2>&1 | grep --text '< Content-Encoding'
    ${CLICKHOUSE_CURL} -vsS -H 'Accept-Encoding: deflate' "${CLICKHOUSE_URL}&enable_http_compression=1&http_zlib_compression_level=${level}" -d "${QUERY}" 2>&1 | grep --text '< Content-Encoding'

    # And it must decompress back to the correct result.
    ${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "${CLICKHOUSE_URL}&enable_http_compression=1&http_zlib_compression_level=${level}" -d "${QUERY}" | gzip -d
    ${CLICKHOUSE_CURL} -sS --compressed -H 'Accept-Encoding: deflate' "${CLICKHOUSE_URL}&enable_http_compression=1&http_zlib_compression_level=${level}" -d "${QUERY}"
done
