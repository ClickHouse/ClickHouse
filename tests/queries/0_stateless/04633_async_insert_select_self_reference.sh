#!/usr/bin/env bash
# Checks table names in asynchronous_insert_log; no thread/topology dependence, so no tags needed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A self referencing `INSERT ... SELECT` takes the async insert queue route: the SELECT releases its
# table locks before it waits for the flush. Settings are pinned per query so the randomizer cannot
# split the block into several.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_self"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_04633_self (n UInt64) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_04633_self SETTINGS async_insert = 0 VALUES (1), (2), (3)"
query_id="${CLICKHOUSE_DATABASE}_04633_self"
${CLICKHOUSE_CLIENT} --query_id "$query_id" -q "
    INSERT INTO test_04633_self SELECT n FROM test_04633_self
    SETTINGS async_insert = 1, wait_for_async_insert = 1,
             max_threads = 1, max_insert_threads = 1,
             max_block_size = 1000, preferred_block_size_bytes = 0
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04633_self"
# Log element is queued after the query returns, so one flush can miss it:
# https://github.com/ClickHouse/ClickHouse/issues/84364. Retry bounded so a real failure prints 0.
# Filtered by query_id, not just table: the AsyncInsert CI profile forces async_insert = 1 for the
# seed insert too, which would otherwise leave its own row in the log.
reached_queue=0
for _ in $(seq 1 60); do
	${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
	reached_queue=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() >= 1
        FROM system.asynchronous_insert_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND database = currentDatabase()
          AND table = 'test_04633_self'
          AND query_id = '$query_id'
    ")
	[ "$reached_queue" = 1 ] && break
	sleep 0.5
done
echo "$reached_queue"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_self"
