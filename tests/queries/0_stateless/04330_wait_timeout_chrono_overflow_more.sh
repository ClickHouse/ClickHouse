#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04330"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_04330 (x UInt64) ENGINE = MergeTree ORDER BY x"

# wait_for_async_insert = 1 makes the server wait on the insert future with
# std::future::wait_for(wait_for_async_insert_timeout). A huge timeout (1e11 s -> 1e14 ms)
# overflows the ms-to-ns conversion unless it is saturated. The small busy timeout flushes
# the batch quickly, so the wait returns right away once the overflow site is clamped.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&wait_for_async_insert_timeout=100000000000&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_max_ms=200" \
    --data-binary "INSERT INTO t_04330 VALUES (1)"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_04330"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_04330"
