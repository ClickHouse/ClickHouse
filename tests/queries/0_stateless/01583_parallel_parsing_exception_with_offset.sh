#!/usr/bin/env bash
# Tags: no-debug, no-asan, no-tsan, no-msan, no-ubsan
# Test is flaky in debug/sanitizer builds because large data transfer
# can cause connection timeouts before the parsing error is reported.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS check;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE check (x UInt64) ENGINE = Memory;"

(seq 1 2000000; echo 'hello'; seq 1 20000000) | $CLICKHOUSE_CLIENT --input_format_parallel_parsing=1 --min_chunk_bytes_for_parallel_parsing=1000 --query="INSERT INTO check(x) FORMAT TSV " 2>&1 | grep -q "(at row 2000001)" && echo 'OK' || echo 'FAIL' ||:

$CLICKHOUSE_CLIENT --query="DROP TABLE check;"
