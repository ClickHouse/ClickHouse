#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04329"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_04329 (x UInt64) ENGINE = MergeTree ORDER BY x"

# wait_for_async_insert = 0 so the client does not block on the huge timeout.
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_04329 SETTINGS async_insert = 1, wait_for_async_insert = 0, async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_max_ms = 100000000000000, async_insert_poll_timeout_ms = 100000000000000 VALUES (1)"

${CLICKHOUSE_CLIENT} -q "SELECT 'alive'"

# Table-scoped flush drains the queued ~1-year-deadline batch (it does not set the global
# flush_stopped) so it cannot leak into later async-insert tests.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE t_04329"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_04329"
