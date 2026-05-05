#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DROP_DB="${CLICKHOUSE_DATABASE}_fp_drop"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT drop_database_before_exclusive_ddl_lock" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DROP_DB}" 2>/dev/null || true
}
trap cleanup EXIT

# ──────────────────────────────────────────────────────────────────────────────
# Static checks
# ──────────────────────────────────────────────────────────────────────────────

$CLICKHOUSE_CLIENT -nmq "
-- Basic: table exists and returns rows
SELECT count() > 0 FROM system.fail_points;

-- Schema check: verify columns and types
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'fail_points' ORDER BY position;

-- All four types are present
SELECT type, count() > 0 FROM system.fail_points GROUP BY type ORDER BY type;

-- Filtering by type works
SELECT count() > 0 FROM system.fail_points WHERE type = 'pauseable';

-- Filtering by name with LIKE
SELECT count() > 0 FROM system.fail_points WHERE name LIKE '%smt_%';
"

# ──────────────────────────────────────────────────────────────────────────────
# Concurrent smoke test: verify ENABLE / WAIT / NOTIFY / DISABLE round-trip.
#
# We use drop_database_before_exclusive_ddl_lock (PAUSEABLE_ONCE): it fires
# inside DROP DATABASE after tables are processed but before the exclusive DDL
# lock is acquired.
# ──────────────────────────────────────────────────────────────────────────────

FP="drop_database_before_exclusive_ddl_lock"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DROP_DB}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DROP_DB}.t (n UInt64) ENGINE = Memory"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT ${FP}"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DROP_DB}" &
DROP_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT ${FP}"

wait $DROP_PID
echo "OK"
