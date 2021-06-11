#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

echo -ne '\x00\x94\x6f\x8d\x34\x82\xb8\x23\x9f\x3b\x47\xbb\x2d\x9d\xa3\x05\x91\x00\x00\x00\x00\x02\x00\x00\x00\x02\x02\x02\x02\x14\x00\x00\x00\x0b\x00\x00\x00\x02\x0b\x00\x00\x00\x02\x00\x00\x00\x31\x0a' | 
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&decompress=1&http_native_compression_disable_checksumming_on_decompress=1" --data-binary @- 2>&1 | grep -oF 'Exc'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "SELECT 'Ok.'"
