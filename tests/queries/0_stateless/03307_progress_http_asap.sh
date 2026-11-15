#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The first chunk with progress is small, meaning it was sent as soon as it is ready:
${CLICKHOUSE_CURL} --no-buffer -s "${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&default_format=JSONEachRowWithProgress&enable_http_compression=1&max_block_size=1&interactive_delay=0" -H 'Accept-Encoding: zstd' -d "SELECT count() FROM system.numbers" --raw --output - |
    head -n1 | ${CLICKHOUSE_LOCAL} --input-format CSV --structure 'x String' --query "SELECT length(unhex(x)) = 1 ? '1' : x, reinterpretAsUInt8(unhex(x)) <= 100 FROM table"
