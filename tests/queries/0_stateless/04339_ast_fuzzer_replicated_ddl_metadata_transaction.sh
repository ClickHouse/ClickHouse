#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# no-fasttest: needs a Replicated database (ZooKeeper), not available in fast test.

# Regression test for the ZooKeeperMetadataTransaction leak in executeASTFuzzerQueries.
#
# When the server-side AST fuzzer (ast_fuzzer_runs > 0, ast_fuzzer_any_query = 1) runs inside a
# replicated-DDL execution, DatabaseReplicatedDDLWorker re-executes the DDL entry whose serialized
# settings still contain ast_fuzzer_runs, so executeASTFuzzerQueries fires again on a context that
# holds the entry's live ZooKeeperMetadataTransaction. The fuzzed follow-up DDL then reaches
# DatabaseReplicated::commit* -> ZooKeeperMetadataTransaction::addOp on an already-executed
# transaction and throws "Cannot add ZooKeeper operation because query is executed" (a LOGICAL_ERROR
# that aborts debug/sanitizer builds). The fuzzer now detaches metadata_transaction from the fuzz
# context copies, so the fuzzed queries no longer inherit the in-flight DDL transaction.

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

# The fuzzed replicated DDL: ast_fuzzer_any_query = 1 makes the fuzzer mutate this non-read-only
# query, and the setting is serialized into the ZK DDL entry so it re-fuzzes inside DDLWorker on
# the entry's live metadata transaction. A unique query id lets us attribute the ProfileEvent
# below to exactly this statement, so the test is safe to run in parallel with itself. Fuzzed
# follow-up queries log their own internal errors (fatal-level silences that noise); the outer
# ALTER itself succeeds, so stdout stays empty here.
qid="04339_${CLICKHOUSE_DATABASE}_$$"
${ddl} --send_logs_level=fatal --query_id="${qid}" -q "ALTER TABLE ${db}.t ADD COLUMN IF NOT EXISTS b UInt64 SETTINGS ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1"

# The server must still be alive after the fuzzed replicated DDL.
${CLICKHOUSE_CLIENT} -q "SELECT 'alive'"

# Build-mode-independent proof of the fix. 'alive' above only proves the server survived, which a
# release build does even without the fix (the LOGICAL_ERROR is caught inside the fuzzer). Instead
# assert the strip fired: ASTFuzzerClearedMetadataTransaction is bumped only when
# resetZooKeeperMetadataTransaction() detaches a live transaction. It is attributed to this ALTER
# by initial_query_id (client statement + DDLWorker re-execution; fuzzed sub-queries get fresh ids
# and are excluded). Without the fix the counter stays 0 and this assertion flips to 0.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
# enable_parallel_replicas = 0: single-node introspection of system.query_log; keep the CI
# randomizer from turning it into a distributed read over the parallel_replicas cluster.
# current_database = currentDatabase(): required by the query_log style rule and enough here
# because the client (initial) row of the fuzzed ALTER carries the counter and runs in the
# current database; the initial_query_id scope already pins it to this exact statement.
${CLICKHOUSE_CLIENT} -q "
    SELECT 'metadata_transaction_detached',
           sum(ProfileEvents['ASTFuzzerClearedMetadataTransaction']) > 0
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
