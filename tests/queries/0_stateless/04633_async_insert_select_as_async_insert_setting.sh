#!/usr/bin/env bash
# Tags: long
#   long: case 1 waits for the async queue flush and polls asynchronous_insert_log.

# async_insert_select_as_async_insert gates whether a user INSERT ... SELECT may use the async insert
# queue. On (default): an eligible single-block INSERT ... SELECT with async_insert = 1 goes async.
# Off, or compatibility below the feature version: it always runs synchronously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

async_log_count() {
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count()
        FROM system.asynchronous_insert_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND database = currentDatabase()
          AND table = '$1'
    "
}

# Case 1: on -> eligible INSERT ... SELECT takes the async queue route.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_as_async_on"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_as_async_on (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 --async_insert_select_as_async_insert=1 -q "
    INSERT INTO test_as_async_on SELECT number FROM numbers(3)
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_as_async_on"
# The entry may not be visible after just one flush; retry a few times as a safety net.
for _ in $(seq 1 10); do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    [ "$(async_log_count test_as_async_on)" -ge 1 ] && break
    sleep 0.5
done
echo "on async_log >= 1: $([ "$(async_log_count test_as_async_on)" -ge 1 ] && echo 1 || echo 0)"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_as_async_on"

# Case 2: off -> synchronous, no asynchronous_insert_log entry.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_as_async_off"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_as_async_off (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 --async_insert_select_as_async_insert=0 -q "
    INSERT INTO test_as_async_off SELECT number FROM numbers(3)
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_as_async_off"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
echo "off async_log: $(async_log_count test_as_async_off)"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_as_async_off"

# Case 3: compatibility below the feature version restores the old behavior (always synchronous).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_as_async_compat"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_as_async_compat (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 --compatibility=25.8 -q "
    INSERT INTO test_as_async_compat SELECT number FROM numbers(3)
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_as_async_compat"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
echo "compat async_log: $(async_log_count test_as_async_compat)"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_as_async_compat"
