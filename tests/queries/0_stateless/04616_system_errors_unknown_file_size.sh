#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the test checks that a global counter in system.errors does not change.

# Test for issue #92884: reading from url() over a chunked HTTP response (without a
# Content-Length header) incremented the UNKNOWN_FILE_SIZE counter in system.errors
# even though the query succeeded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1"

before=$($CLICKHOUSE_CLIENT -q "SELECT ifNull(sum(value), 0) FROM system.errors WHERE name = 'UNKNOWN_FILE_SIZE'")
# max_download_threads > 1 is required to reach the file size probe in FormatFactory::wrapReadBufferIfNeeded
$CLICKHOUSE_CLIENT -q "SELECT * FROM url('${url}', 'TSV', 'x UInt8') SETTINGS max_download_threads = 4"
after=$($CLICKHOUSE_CLIENT -q "SELECT ifNull(sum(value), 0) FROM system.errors WHERE name = 'UNKNOWN_FILE_SIZE'")

echo $((after - before))
