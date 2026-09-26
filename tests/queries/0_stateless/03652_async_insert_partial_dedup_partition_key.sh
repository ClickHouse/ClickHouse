#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the async insert queue to coalesce several tokens into one flush.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/110604
# Partial async-insert deduplication into a partitioned MergeTree with a dedup log
# used to abort with "Invalid partition key size: 0" (LOGICAL_ERROR): writeTempPart
# moves the partition value out of the block on the first write, and the dedup-conflict
# retry re-wrote the same block whose partition value was already emptied.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS minimal"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE minimal (id String, event_time DateTime64(6, 'UTC'))
ENGINE = MergeTree PARTITION BY toYYYYMMDD(event_time) ORDER BY (event_time, id)
SETTINGS non_replicated_deduplication_window = 10000"

# Seed the dedup log with token m-X so that a later coalesced async batch containing
# m-X becomes a PARTIAL duplicate (only some tokens are duplicates).
$CLICKHOUSE_CLIENT --insert_deduplicate=1 --insert_deduplication_token='m-X' \
    -q "INSERT INTO minimal VALUES ('r1', '2026-07-15 10:00:00')"

# Queue five async inserts with wait_for_async_insert=0 and a busy timeout long enough that
# nothing auto-fires, then trigger one explicit table-scoped flush. insert_deduplication_token
# is excluded from the async queue key (settings_to_skip), so the five different tokens land in
# ONE flush batch. m-X is a duplicate -> partial dedup -> MergeTreeSink::finishDelayedChunk
# rewrites the filtered part (the fixed path). No busy-timeout race.
for t in X Y Z W V; do
    $CLICKHOUSE_CLIENT --async_insert=1 --wait_for_async_insert=0 \
        --async_insert_busy_timeout_min_ms=600000 --async_insert_busy_timeout_max_ms=600000 \
        --async_insert_use_adaptive_busy_timeout=0 \
        --insert_deduplicate=1 --insert_deduplication_token="m-$t" \
        -q "INSERT INTO minimal VALUES ('m_$t', '2026-07-15 10:00:0${#t}')"
done
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE minimal"

# Before the fix the server aborted; now the insert succeeds and m-X is deduplicated.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM minimal"
$CLICKHOUSE_CLIENT -q "SELECT id FROM minimal ORDER BY id"

# Prove the partial-dedup retry actually fired instead of the five entries flushing separately:
# exactly one coalesced AsyncInsertFlush that carried all five rows AND removed a duplicate token.
# With separate flushes each flush would have AsyncInsertRows = 1, so this returns 0 and the test
# fails, guarding against silently un-covering finishDelayedChunk's rewrite path.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT 'partial_dedup_retry_fired', count()
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND has(databases, currentDatabase())
  AND has(tables, currentDatabase() || '.minimal')
  AND type != 'QueryStart'
  AND query_kind = 'AsyncInsertFlush'
  AND ProfileEvents['AsyncInsertRows'] = 5
  AND ProfileEvents['DuplicatedAsyncInserts'] > 0"

$CLICKHOUSE_CLIENT -q "DROP TABLE minimal"
