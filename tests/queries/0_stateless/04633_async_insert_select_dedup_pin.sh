#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: `InterpreterInsertQuery` pins the INSERT ... SELECT deduplication decision so
# the async insert queue route and the synchronous fallback route never disagree on whether to
# deduplicate the same query.
#
# With `deduplicate_insert = 'backward_compatible_choice'`, `isDeduplicationEnabledForInsert()`
# resolves through `insert_deduplicate` for sync inserts and `async_insert_deduplicate` for async
# ones. `deduplicate_insert_select = 'enable_even_for_bad_queries'` makes the SELECT-side decision
# equal the sync decision unconditionally, so with `insert_deduplicate = 1` and
# `async_insert_deduplicate = 0` the two routes disagree (true vs false) while looking consistent
# to the old "differs from sync default" guard. Without the pin, the async flush thread resolves
# `async_insert_deduplicate = false` on its own and skips deduplication, so a retried single-block
# INSERT ... SELECT duplicates rows.
#
# `deduplicate_insert = 'backward_compatible_choice'` is set directly instead of via
# `compatibility`, to avoid reverting unrelated settings from every release in between.
#
# Every other setting affecting routing/eligibility is pinned in the query's own SETTINGS clause
# so the settings randomizer cannot change which route is taken.
SHARED_SETTINGS="async_insert = 1, wait_for_async_insert = 1, insert_deduplicate = 1, async_insert_deduplicate = 0, deduplicate_insert = 'backward_compatible_choice', deduplicate_insert_select = 'enable_even_for_bad_queries', max_insert_threads = 1, max_threads = 1, insert_quorum = 0, implicit_transaction = 0, parallel_distributed_insert_select = 0, optimize_trivial_insert_select = 0"

# --- Queue route: a single-block SELECT (10 rows, large max_block_size) takes the async queue path.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_dedup_queue_route"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_04633_dedup_queue_route (id UInt64, data String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS non_replicated_deduplication_window = 1000
"
for _ in 1 2; do
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_04633_dedup_queue_route
    SELECT number AS id, toString(number) AS data FROM numbers(10)
    SETTINGS ${SHARED_SETTINGS}, max_block_size = 1000000
"
done
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04633_dedup_queue_route"
# Only that the route was taken matters, not the flush cycle count. One flush can miss the log
# element (https://github.com/ClickHouse/ClickHouse/issues/84364), so retry until it lands.
reached_queue=0
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    reached_queue=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() > 0
        FROM system.asynchronous_insert_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND database = currentDatabase()
          AND table = 'test_04633_dedup_queue_route'
    ")
    [ "$reached_queue" = 1 ] && break
    sleep 0.5
done
echo "$reached_queue"

# --- Synchronous fallback route: same query and settings, but `max_block_size = 1` forces 10
# single-row blocks, taking the multi-block fallback path instead of the queue. The dedup pin
# applies to this route too since queue eligibility is decided upfront, independent of block count.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_04633_dedup_sync_route"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_04633_dedup_sync_route (id UInt64, data String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS non_replicated_deduplication_window = 1000
"
for _ in 1 2; do
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO test_04633_dedup_sync_route
    SELECT number AS id, toString(number) AS data FROM numbers(10)
    SETTINGS ${SHARED_SETTINGS}, max_block_size = 1
"
done
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM test_04633_dedup_sync_route"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count()
    FROM system.asynchronous_insert_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND database = currentDatabase()
      AND table = 'test_04633_dedup_sync_route'
"

# Both routes must agree: retrying the duplicate INSERT ... SELECT leaves the same row count
# on both tables. If the pin regresses, this becomes 20 vs 10 and the check below returns 0.
${CLICKHOUSE_CLIENT} -q "
    SELECT (SELECT count() FROM test_04633_dedup_queue_route) = (SELECT count() FROM test_04633_dedup_sync_route)
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_dedup_queue_route"
${CLICKHOUSE_CLIENT} -q "DROP TABLE test_04633_dedup_sync_route"
