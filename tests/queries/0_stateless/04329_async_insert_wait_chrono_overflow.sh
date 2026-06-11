#!/usr/bin/env bash
# Tags: no-parallel
# A huge async_insert_busy_timeout_max_ms reached condition_variable::wait_for and the
# `now + timeout_ms` deadline in the async insert queue. The value (1e14 ms) is below the
# SettingFieldMilliseconds saturation cap but still overflows the millisecond-to-nanosecond
# conversion, which aborted the server under UBSan. The wait timeouts are now clamped, so the
# insert must succeed and the server must stay alive instead of crashing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04329"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_04329 (x UInt64) ENGINE = MergeTree ORDER BY x"

# Fire-and-forget insert: the background deadline thread builds `now + timeout_ms` and waits with
# the same huge timeout, which is where the overflow used to abort the server.
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_04329 SETTINGS async_insert = 1, wait_for_async_insert = 0, async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_max_ms = 100000000000000 VALUES (1)"

# Synchronous insert with huge poll/min/max timeouts also exercises the adaptive wait path.
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_04329 SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_busy_timeout_max_ms = 100000000000000, async_insert_poll_timeout_ms = 100000000000000 VALUES (2)"

# The server must still be responsive (it would be dead here on an unclamped UBSan build).
${CLICKHOUSE_CLIENT} -q "SELECT 'alive'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_04329"
