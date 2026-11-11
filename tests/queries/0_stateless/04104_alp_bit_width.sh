#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

(printf '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\234\232\040\0\0\0\040\0\0\001\010\0\004\0\0\0\0A\0\0\0\0\0\0\0\0'; head -c 8320 /dev/zero) \
    | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&decompress=1&http_native_compression_disable_checksumming_on_decompress=1" --data-binary @- \
    | grep -c "Code: 271"
