#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL&http_wait_end_of_query=0&http_response_buffer_size=0" -d "SELECT number FROM system.numbers WHERE number % 1234567890 = 1 SETTINGS max_block_size = 100000, max_rows_to_read = 0, max_bytes_to_read = 0, interactive_delay = 0 FORMAT JSONEachRowWithProgress" 2>/dev/null | head -n10 | sed -r -e 's/,?"elapsed_ns":"[0-9]+"//'
