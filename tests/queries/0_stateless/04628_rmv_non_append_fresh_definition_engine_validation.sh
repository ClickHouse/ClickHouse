#!/usr/bin/env bash
# Tags: atomic-database, no-fasttest
# no-fasttest: BACKUP/RESTORE needs the backups disk from the full test config.
# A non-APPEND refreshable materialized view must live in an Atomic or Replicated database. CREATE
# enforced this, but a full-definition ATTACH and a RESTORE (which can remap the view into another
# database) bypassed the check, producing a Nil-UUID view whose refresh aborted the server in a
# Replicated-DDL parent-table lookup. All fresh definition paths are validated the same way now.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# $CLICKHOUSE_DATABASE is created by the runner (Atomic); create the Memory database ourselves.
MEM_DB="${CLICKHOUSE_DATABASE}_mem"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${MEM_DB} ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.target0 (c0 Int32) ENGINE = MergeTree ORDER BY c0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.target1 (c0 Int32) ENGINE = MergeTree ORDER BY c0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.target2 (c0 Int32) ENGINE = MergeTree ORDER BY c0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.target_ok (c0 Int32) ENGINE = MergeTree ORDER BY c0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.target3 (c0 Int32) ENGINE = MergeTree ORDER BY c0"

# CREATE of a non-APPEND refreshable MV in a Memory database is rejected (unchanged behavior).
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW ${MEM_DB}.v0 REFRESH AFTER 1 YEAR TO ${CLICKHOUSE_DATABASE}.target0 (c0 Int32) AS SELECT 1 AS c0" 2>&1 \
    | grep -q "INCORRECT_QUERY" && echo "create_rejected"

# Full-definition ATTACH used to bypass the check (this is the query that reproduced the crash).
$CLICKHOUSE_CLIENT -q "ATTACH MATERIALIZED VIEW ${MEM_DB}.v1 REFRESH AFTER 1 YEAR TO ${CLICKHOUSE_DATABASE}.target1 (c0 Int32) AS SELECT 1 AS c0" 2>&1 \
    | grep -q "INCORRECT_QUERY" && echo "attach_rejected"

# APPEND refreshable MVs are allowed in any database engine (no over-blocking). Full-definition
# ATTACH prints a "not recommended" warning to stderr, which is expected here; drop it.
$CLICKHOUSE_CLIENT --send_logs_level=none -q "ATTACH MATERIALIZED VIEW ${MEM_DB}.v2 REFRESH AFTER 1 YEAR APPEND TO ${CLICKHOUSE_DATABASE}.target2 (c0 Int32) AS SELECT 1 AS c0" 2>/dev/null \
    && echo "append_allowed"

# A non-APPEND refreshable MV in an Atomic database stays allowed.
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.v_ok REFRESH AFTER 1 YEAR TO ${CLICKHOUSE_DATABASE}.target_ok (c0 Int32) AS SELECT 1 AS c0" \
    && echo "atomic_allowed"

# RESTORE that remaps the view into a Memory database is fresh input too.
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.v3 REFRESH AFTER 1 YEAR TO ${CLICKHOUSE_DATABASE}.target3 (c0 Int32) AS SELECT 1 AS c0"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE ${CLICKHOUSE_DATABASE}.v3 TO Disk('backups', '${CLICKHOUSE_DATABASE}/v3') FORMAT Null" > /dev/null
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.v3 AS ${MEM_DB}.v3 FROM Disk('backups', '${CLICKHOUSE_DATABASE}/v3')" 2>&1 \
    | grep -q "INCORRECT_QUERY" && echo "restore_rejected"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${MEM_DB} SYNC"
