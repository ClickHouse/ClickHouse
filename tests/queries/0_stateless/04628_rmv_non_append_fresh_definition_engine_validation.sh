#!/usr/bin/env bash
# Tags: atomic-database, no-fasttest, zookeeper, no-replicated-database, no-ordinary-database
# no-fasttest: BACKUP/RESTORE needs the backups disk from the full test config.
# no-replicated-database, no-ordinary-database: the test creates its own Replicated and Ordinary databases.
# A non-APPEND refreshable materialized view must live in an Atomic or Replicated database. CREATE
# enforced this, but a full-definition ATTACH and a RESTORE (which can remap the view into another
# database) bypassed the check, producing a Nil-UUID view whose refresh aborted the server in a
# Replicated-DDL parent-table lookup. All fresh definition paths are validated the same way now.
# Definitions stored before the validation existed must keep loading, their refresh must be refused
# cleanly, and their backups must stay restorable; a cross-engine RENAME reaches that state without
# InterpreterCreateQuery.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# $CLICKHOUSE_DATABASE is created by the runner (Atomic); create the other databases ourselves.
MEM_DB="${CLICKHOUSE_DATABASE}_mem"
ORD_DB="${CLICKHOUSE_DATABASE}_ord"
RDB="${CLICKHOUSE_DATABASE}_rdb"

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

# Metadata written before the validation existed must keep working. Moving a view from an Atomic to
# an Ordinary database clears its UUID and installs the definition through the target database's own
# createTable, so it reaches that state without going through InterpreterCreateQuery.
# send_logs_level=fatal drops the once-per-server "Ordinary engine is deprecated" warning.
$CLICKHOUSE_CLIENT --send_logs_level=fatal --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE ${ORD_DB} ENGINE = Ordinary"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE DATABASE ${RDB} ENGINE = Replicated('/test/{database}/rmv_fresh_definition', 'shard1', 'replica1')"
# The target is deliberately in the Replicated database: that is what routes the refresh's
# CREATE OR REPLACE of the temporary inner table through the Replicated DDL log, where the view's
# UUID is looked up as the parent table.
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE TABLE ${RDB}.legacy_target (c0 Int32) ENGINE = MergeTree ORDER BY c0"
# EMPTY so no refresh runs before the rename.
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.legacy_v REFRESH EVERY 100 YEAR TO ${RDB}.legacy_target (c0 Int32) EMPTY AS SELECT 1 AS c0"
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${CLICKHOUSE_DATABASE}.legacy_v TO ${ORD_DB}.legacy_v"
$CLICKHOUSE_CLIENT -q "SELECT 'legacy_uuid_is_nil' FROM system.tables WHERE database = '${ORD_DB}' AND name = 'legacy_v' AND uuid = toUUID('00000000-0000-0000-0000-000000000000')"

# The stored definition still loads: a short ATTACH rewrites the query from metadata, so it must not
# be validated as a fresh definition.
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${ORD_DB}.legacy_v"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${ORD_DB}.legacy_v"
$CLICKHOUSE_CLIENT -q "SELECT 'legacy_short_attach_ok' FROM system.tables WHERE database = '${ORD_DB}' AND name = 'legacy_v'"

# Its refresh is refused cleanly instead of tripping the parent-table UUID assertion.
$CLICKHOUSE_CLIENT -q "SYSTEM REFRESH VIEW ${ORD_DB}.legacy_v"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT VIEW ${ORD_DB}.legacy_v" 2>&1 \
    | grep -q "Parent table doesn't exist" && echo "legacy_refresh_refused"

# A backup of such a definition must stay restorable into the same database it was taken from: it
# carries no UUID, so the restore is replaying a definition that already lived without one rather
# than remapping a UUID view into a database that cannot hold its UUID.
$CLICKHOUSE_CLIENT -q "BACKUP TABLE ${ORD_DB}.legacy_v TO Disk('backups', '${CLICKHOUSE_DATABASE}/legacy_v') FORMAT Null" > /dev/null
$CLICKHOUSE_CLIENT --force_remove_data_recursively_on_drop=1 -q "DROP TABLE ${ORD_DB}.legacy_v SYNC"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${ORD_DB}.legacy_v FROM Disk('backups', '${CLICKHOUSE_DATABASE}/legacy_v')" > /dev/null \
    && echo "legacy_restore_ok"

# Restoring it under another name is accepted for the same reason (there is no UUID to lose), and the
# copy stays harmless: its refresh is refused by the same Nil guard rather than aborting the server.
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${ORD_DB}.legacy_v AS ${ORD_DB}.legacy_v_copy FROM Disk('backups', '${CLICKHOUSE_DATABASE}/legacy_v')" > /dev/null \
    && echo "legacy_restore_as_ok"
$CLICKHOUSE_CLIENT -q "SYSTEM REFRESH VIEW ${ORD_DB}.legacy_v_copy"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT VIEW ${ORD_DB}.legacy_v_copy" 2>&1 \
    | grep -q "Parent table doesn't exist" && echo "legacy_restore_as_refresh_refused"

# A server that aborted during any of those refreshes cannot answer this.
$CLICKHOUSE_CLIENT -q "SELECT 'server_alive'"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${MEM_DB} SYNC"
# force_remove_data_recursively_on_drop: an Ordinary database keeps metadata for a detached table,
# so without it a mid-test failure would cascade into a confusing DATABASE_NOT_EMPTY error here.
$CLICKHOUSE_CLIENT --force_remove_data_recursively_on_drop=1 -q "DROP DATABASE IF EXISTS ${ORD_DB} SYNC"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "DROP DATABASE IF EXISTS ${RDB} SYNC"
for t in v_ok v3 target0 target1 target2 target3 target_ok; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${t} SYNC"
done
