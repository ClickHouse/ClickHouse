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
# context copies, so the fuzzed queries no longer inherit the in-flight DDL transaction and the
# server stays up. Without the fix this test crashes the server (the runner then reports it dead).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Reuse the implicit test database when it is already Replicated (the replicated-database CI
# variant); otherwise make a dedicated Replicated database. The dedicated one uses the unique
# per-test ZooKeeper prefix so the test is safe to run in parallel with itself.
db="${CLICKHOUSE_DATABASE}"
if [[ $(${CLICKHOUSE_CLIENT} -q "SELECT engine = 'Replicated' FROM system.databases WHERE name = '${CLICKHOUSE_DATABASE}'") != 1 ]]; then
    db="rdb_${CLICKHOUSE_DATABASE}"
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db} SYNC"
    ${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${db} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb', 's1', 'r1')"
fi

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${db}.t (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a"

# The fuzzed replicated DDL: ast_fuzzer_any_query = 1 makes the fuzzer mutate this non-read-only
# query, and the setting is serialized into the ZK DDL entry so it re-fuzzes inside DDLWorker on
# the entry's live metadata transaction. Fuzzed follow-up queries log their own internal errors
# (fatal-level silences that noise); the outer ALTER itself succeeds, so stdout stays empty here.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "ALTER TABLE ${db}.t ADD COLUMN IF NOT EXISTS b UInt64 SETTINGS ast_fuzzer_runs = 5, ast_fuzzer_any_query = 1"

# The server must still be alive after the fuzzed replicated DDL.
${CLICKHOUSE_CLIENT} -q "SELECT 'alive'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${db}.t SYNC"
if [[ "${db}" != "${CLICKHOUSE_DATABASE}" ]]; then
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db} SYNC"
fi
