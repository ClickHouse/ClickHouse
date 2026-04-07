#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

printf '\x41\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x90\x1a\x00\x00\x00\xe8\x03\x00\x00(\xb5/\xfd \x08A\x00\x00SELECT 1' \
    | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&decompress=1&http_native_compression_disable_checksumming_on_decompress=1" --data-binary @- | grep -c "The size after decompression"