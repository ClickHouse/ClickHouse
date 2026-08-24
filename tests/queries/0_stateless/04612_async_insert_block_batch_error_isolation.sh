#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Per-entry error isolation on the async insert queue flush.
# Two async entries with the same query share one bucket; a concurrent `MODIFY COLUMN` makes
# only one unconvertible at flush. Both the parsing path (`INSERT ... VALUES`, `data_kind =
# Parsed`, isolated via `on_error` in `processEntriesWithParsing`) and the block path (native
# block, `data_kind = Preprocessed`, isolated in `processPreprocessedEntries`) must isolate
# the bad entry so the valid sibling (id=1, '100' -> 100) still lands.
# Before the block-path fix the unconvertible block failed the whole batch.

async=(--async_insert=1 --wait_for_async_insert=0 --async_insert_busy_timeout_max_ms=300000 --async_insert_busy_timeout_min_ms=300000 --async_insert_use_adaptive_busy_timeout=0)

echo "=== VALUES (Parsed): bad entry is isolated, good entry survives ==="
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_async_iso_values"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_async_iso_values (id Int64, amount String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT "${async[@]}" -q "INSERT INTO t_async_iso_values VALUES (1, '100')"
$CLICKHOUSE_CLIENT "${async[@]}" -q "INSERT INTO t_async_iso_values VALUES (2, 'hello')"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_async_iso_values MODIFY COLUMN amount Int64"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE t_async_iso_values"
$CLICKHOUSE_CLIENT -q "SELECT id, amount FROM t_async_iso_values ORDER BY id"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_async_iso_values"

echo "=== native block (Preprocessed): bad entry is isolated, good entry survives ==="
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_async_iso_block"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_async_iso_block (id Int64, amount String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "SELECT toInt64(1) AS id, '100'::String AS amount FORMAT Native"   | $CLICKHOUSE_CLIENT "${async[@]}" -q "INSERT INTO t_async_iso_block FORMAT Native"
$CLICKHOUSE_CLIENT -q "SELECT toInt64(2) AS id, 'hello'::String AS amount FORMAT Native" | $CLICKHOUSE_CLIENT "${async[@]}" -q "INSERT INTO t_async_iso_block FORMAT Native"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_async_iso_block MODIFY COLUMN amount Int64"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE t_async_iso_block"
$CLICKHOUSE_CLIENT -q "SELECT id, amount FROM t_async_iso_block ORDER BY id"
# Verify the bad entry is recorded as a failure in the log, not silently dropped.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
$CLICKHOUSE_CLIENT -q "SELECT data_kind, status, flush_time_microseconds > 0 AS has_flush_time FROM system.asynchronous_insert_log WHERE database = currentDatabase() AND table = 't_async_iso_block' AND event_date >= yesterday() AND event_time >= now() - 600 ORDER BY status"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_async_iso_block"
