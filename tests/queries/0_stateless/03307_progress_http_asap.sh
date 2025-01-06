#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The first chunk with progress is small, meaning it was sent as soon as it is ready:
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}&default_format=JSONEachRowWithProgress&enable_http_compression=1" -H 'Accept-Encoding: zstd' -d "SELECT count() FROM system.numbers SETTINGS max_block_size = 1" --raw --output - |
    head -n5 | xxd | grep -o '3339 0d0a 28b5 2ffd 0458 8001 007b 2270'
