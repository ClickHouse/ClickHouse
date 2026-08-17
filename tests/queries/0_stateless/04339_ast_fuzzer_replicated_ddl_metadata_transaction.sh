#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# no-fasttest: needs a Replicated database (ZooKeeper), not available in fast test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# distributed_ddl_output_mode=none on every DDL: with the default 'throw' mode a replicated DDL
# prints a per-replica status row ("s1 r1 OK 0 0") to stdout, which is not part of this test's
# checked output. Pin it so the DDL below never pollutes stdout regardless of the CI profile.
ddl="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none"

# Reuse the implicit test database when it is already Replicated (the replicated-database CI
# variant); otherwise make a dedicated Replicated database. The dedicated one uses the unique
# per-test ZooKeeper prefix so the test is safe to run in parallel with itself.
db="${CLICKHOUSE_DATABASE}"
if [[ $(${CLICKHOUSE_CLIENT} -q "SELECT engine = 'Replicated' FROM system.databases WHERE name = '${CLICKHOUSE_DATABASE}'") != 1 ]]; then
    db="rdb_${CLICKHOUSE_DATABASE}"
    ${ddl} -q "DROP DATABASE IF EXISTS ${db} SYNC"
    ${ddl} -q "CREATE DATABASE ${db} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb', 's1', 'r1')"
fi

${ddl} -q "CREATE TABLE ${db}.t (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a"

# ast_fuzzer_any_query = 1 fuzzes this non-read-only ALTER, and the setting is serialized into the ZK
# DDL entry, so DatabaseReplicatedDDLWorker re-fuzzes it while re-executing the entry on the entry's
# live ZooKeeperMetadataTransaction. The unique query id attributes the ProfileEvent below to this
# statement (the client query and the DDLWorker re-execution share initial_query_id; fuzzed sub-queries
# get fresh ids). Fuzzed follow-ups log their own internal errors; the outer ALTER succeeds, so stdout
# stays empty here.
qid="04339_${CLICKHOUSE_DATABASE}_$$"
${ddl} --send_logs_level=fatal --query_id="${qid}" -q "ALTER TABLE ${db}.t ADD COLUMN IF NOT EXISTS b UInt64 SETTINGS ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1"

# The server must still be alive after the fuzzed replicated DDL.
${CLICKHOUSE_CLIENT} -q "SELECT 'alive'"

# Build-mode-independent proof of the fix. 'alive' above only proves the server survived, which a
# release build does even without the fix (the LOGICAL_ERROR is caught inside the fuzzer). Instead
# assert the fuzzer declined to fuzz during the internal DDL re-execution:
# ASTFuzzerSkippedReplicatedDDLInternal is bumped only when executeASTFuzzerQueries returns early on a
# context that holds a live metadata transaction (the DDLWorker re-execution row). It is attributed to
# this ALTER by initial_query_id (client statement + DDLWorker re-execution; fuzzed sub-queries get
# fresh ids and are excluded). Without the fix the counter stays 0 and this assertion flips to 0.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
# enable_parallel_replicas = 0: single-node introspection of system.query_log; keep the CI randomizer
# from turning it into a distributed read over the parallel_replicas cluster.
# current_database = currentDatabase(): required by the query_log style rule; the DDLWorker re-execution
# row (which carries the counter) runs in the current database, and initial_query_id already pins the
# result to this exact statement.
${CLICKHOUSE_CLIENT} -q "
    SELECT 'fuzzer_skipped_in_replicated_ddl',
           sum(ProfileEvents['ASTFuzzerSkippedReplicatedDDLInternal']) > 0
    FROM system.query_log
    WHERE event_date >= today() - 1
      AND initial_query_id = '${qid}'
      AND current_database = currentDatabase()
      AND type = 'QueryFinish'
    SETTINGS enable_parallel_replicas = 0"

${ddl} -q "DROP TABLE IF EXISTS ${db}.t SYNC"
if [[ "${db}" != "${CLICKHOUSE_DATABASE}" ]]; then
    ${ddl} -q "DROP DATABASE IF EXISTS ${db} SYNC"
fi
