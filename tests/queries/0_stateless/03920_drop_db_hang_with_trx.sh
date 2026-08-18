#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-ordinary-database, no-encrypted-storage
# no-parallel: The PAUSEABLE_ONCE failpoint fires exactly once globally; a concurrent DROP DATABASE
#   from another parallel test could steal the failpoint pause from Q1, causing this test to hang.
# no-replicated-database: Failpoints are single-server; DROP DATABASE behavior inside a replicated
#   database differs (DDL is replicated via ZooKeeper, not executed directly on each replica).
# no-ordinary-database: The bug involves UUID-based waitTableFinallyDropped which is specific to
#   DatabaseAtomic; DatabaseOrdinary uses a different table-dropping mechanism with no UUIDs.
# no-encrypted-storage: Encrypted-storage CI jobs skip installing transactions.xml, so
#   allow_experimental_transactions is unset. The implicit-transaction query then fails before
#   reaching the failpoint, leaving SYSTEM WAIT FAILPOINT PAUSE blocked indefinitely.
# Regression test: implicit-transaction StoragePtr deadlock in DROP DATABASE SYNC.
#
# Bug: executeToDatabaseImpl reuses a single ASTDropQuery object across the tables_to_drop loop.
# When ignore_drop_queries_probability fires for a non-disk table (e.g. Memory), it mutates
# query_for_table.kind to Truncate.  Because the object is shared, subsequent disk tables
# (MergeTree) bypass the storesDataOnDisk() guard and are TRUNCATEd instead of DROPped.
#
# Under implicit_transaction=1 each TRUNCATE calls MergeTreeTransaction::removeOldPart which
# inserts a StoragePtr into the transaction's storages set.  That reference is only released in
# afterFinalize(), i.e. after the transaction commits, which happens after the query returns.
#
# If a concurrent DROP TABLE on the same table adds its UUID to tables_marked_dropped_ids
# before Q1 calls waitTableFinallyDropped, the cleanup task finds isSharedPtrUnique=false
# (Q1's transaction still holds the StoragePtr) and skips cleanup forever.
# Q1 then blocks in waitTableFinallyDropped while holding the reference that prevents cleanup
# from making progress — a circular deadlock(Q1 blocks itself).
#
# The failpoint drop_database_before_exclusive_ddl_lock pauses Q1 after all tables have been
# processed but before detachDatabase is called, giving Q2 time to drop the tables and
# populate tables_marked_dropped_ids deterministically.
#
# Note: implicit_transaction=1 with DROP DATABASE requires
# throw_on_unsupported_query_inside_transaction=0 because InterpreterDropQuery::supportsTransactions()
# returns false for DROP DATABASE (only true for TRUNCATE TABLE).  The implicit transaction is
# still created before the supportsTransactions() check, so
# the transaction context propagates into the TRUNCATE calls inside executeToDatabaseImpl.
# This matches the CI stress test conditions (settings2.xml disables throw_on_unsupported).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_dropdb_hang"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT drop_database_before_exclusive_ddl_lock" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -nm -q "
    CREATE DATABASE ${DB};
    -- 'a_mem' sorts before 'z_mt' in std::map so it is always processed first in the
    -- tables_to_drop loop, ensuring mutation bleeding affects z_mt deterministically.
    CREATE TABLE ${DB}.a_mem (n UInt64) ENGINE = Memory;
    CREATE TABLE ${DB}.z_mt  (n UInt64) ENGINE = MergeTree() ORDER BY n;
    INSERT INTO ${DB}.z_mt VALUES (1);
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT drop_database_before_exclusive_ddl_lock"

# Q1: DROP DATABASE with settings that trigger RC2a without the fix.
#   - ignore_drop_queries_probability=1.0: every non-disk table fires the probability and
#     mutates query_for_table.kind to Truncate (mutation bleeding).
#   - implicit_transaction=1: the TRUNCATE of z_mt runs inside Q1's implicit transaction,
#     causing removeOldPart to insert StoragePtr(z_mt) into the transaction's storages set.
#   - throw_on_unsupported_query_inside_transaction=0: the implicit transaction is still
#     created for DROP DATABASE even though supportsTransactions() returns false, matching
#     the CI stress test conditions.
# Note: allow_experimental_transactions is a server config (not a client setting); the test
#   server already has it enabled.
${CLICKHOUSE_CLIENT} \
    --implicit_transaction=1 \
    --throw_on_unsupported_query_inside_transaction=0 \
    --ignore_drop_queries_probability=1.0 \
    -q "DROP DATABASE IF EXISTS ${DB} SYNC" &
Q1_PID=$!

# Wait until Q1 has processed all tables and is paused just before detachDatabase.
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT drop_database_before_exclusive_ddl_lock PAUSE"

# Drop the tables individually while Q1 is paused.
# Q1's database-level DDL guard does not hold database_ddl_mutex, so table-level DROP TABLE
# queries can still acquire database_ddl_mutex.lock_shared() and proceed.
# These drops add a_mem_uuid and z_mt_uuid to tables_marked_dropped_ids.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.a_mem"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.z_mt"

# Resume Q1.  With the fix: z_mt's uuid_to_wait is Nil (ignored via return {}), so
# waitTableFinallyDropped(Nil) returns immediately.  Q1 completes.
# Without the fix: Q1's implicit transaction holds StoragePtr(z_mt), cleanup cannot proceed,
# and Q1 deadlocks in waitTableFinallyDropped(z_mt_uuid).
${CLICKHOUSE_CLIENT} -q "SYSTEM NOTIFY FAILPOINT drop_database_before_exclusive_ddl_lock"

wait $Q1_PID

echo "No deadlock, DROP DATABASE completed successfully."

# The database must be gone.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${DB}'"
