#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database
# Tag no-parallel: REGULAR failpoints affect the whole server process; parallel tests
#                  running SYSTEM RESTART REPLICA would also fail while the failpoint is active
# Tag no-replicated-database: failpoints are enabled only on one server node
# Regression test for: SYSTEM RESTART REPLICA failure leaves tables_metadata_digest
# inconsistent, causing "Digest does not match" LOGICAL_ERROR on the next DDL.
#
# Scenario: A failpoint makes every StorageFactory::get() call fail inside
# doRestartReplica's retry loop (simulating BuzzHouse MEMORY_LIMIT_EXCEEDED).
# The table is left permanently detached from the in-memory tables map.
#
# Expected (after fix):   TIMEOUT_EXCEEDED / OK
# Observed (before fix):  TIMEOUT_EXCEEDED / LOGICAL_ERROR / OK
#                         (LOGICAL_ERROR: Digest does not match -- debug/sanitizer builds only)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_repl"
ZK_PATH="/test/restart_replica_digest_mismatch/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# 1. Create a Replicated database and a ReplicatedMergeTree table.
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${DB}
    ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')
"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}.t1 (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"

# 2. Make every StorageFactory::get() call inside doRestartReplica throw, simulating
#    the MEMORY_LIMIT_EXCEEDED scenario from the BuzzHouse fuzzer report.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT restart_replica_fail_after_detach"

# 3. SYSTEM RESTART REPLICA fails after exhausting retries.
#    t1 is detached from the tables map but never re-attached.
#    tables_metadata_digest still includes t1's hash — this is the bug.
${CLICKHOUSE_CLIENT} --max_execution_time 2 \
    -q "SYSTEM RESTART REPLICA ${DB}.t1" 2>&1 \
    | grep -o "TIMEOUT_EXCEEDED" | head -1

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT restart_replica_fail_after_detach"

# 4. Force the metadata digest assertion to always run (skips the 1/16 probability gate).
#    In release builds assertDigestWithProbability is compiled out, so this is a no-op
#    and the subsequent DDL always succeeds regardless of the fix.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_replicated_force_metadata_digest_check"

# 5. Run a subsequent DDL.
#    WITHOUT fix (debug/sanitizer builds): assertDigestWithProbability fires, detects
#    local_digest != tables_metadata_digest, throws LOGICAL_ERROR "Digest does not match".
#    WITH fix: tables_metadata_digest was corrected in the error path — DDL succeeds.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}.t2 (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id" 2>&1 \
    | grep -o "LOGICAL_ERROR" | head -1

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_replicated_force_metadata_digest_check"

echo "OK"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB} SYNC" 2>/dev/null || true
