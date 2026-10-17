#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_async_utf"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_async_utf (id UInt64, data String) ENGINE MergeTree ORDER BY id"

python3 -c '
import sys
sys.stdout.buffer.write(b"\xff\xfe")
sys.stdout.buffer.write("{\"id\": 1, \"data\": \"hello\"}\n".encode("utf-16-le"))
sys.stdout.flush()
' | ${CLICKHOUSE_CLIENT} --query "INSERT INTO test_async_utf SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_busy_timeout_ms = 100 FORMAT JSONEachRow"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_async_utf"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_async_utf"
