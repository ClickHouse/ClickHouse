#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database
# Tag no-parallel: enables a REGULAR failpoint that affects the whole server process.
# Tag no-replicated-database: the test creates its own Replicated database and the
#                             failpoint is enabled only on one server node.
# Regression test for: CREATE OR REPLACE / RENAME of a TimeSeries table inside a
# Replicated database left the outer table detached from the in-memory tables map
# while it remained in ZooKeeper and in tables_metadata_digest, causing a
# "Digest does not match" LOGICAL_ERROR (server abort) on the next DDL.
#
# StorageTimeSeries does not support rename. It used to reject the rename inside
# renameInMemory(), which runs after DatabaseAtomic::renameTable() has already
# detached the table and committed the ZooKeeper transaction, breaking the digest
# invariant. The fix rejects the rename early in checkTableCanBeRenamed(), before
# anything is detached or committed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_repl"
ZK_PATH="/test/timeseries_create_or_replace_digest/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${DB}
    ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')
"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --allow_experimental_time_series_table=1 \
    -q "CREATE TABLE ${DB}.ts ENGINE = TimeSeries"

# CREATE OR REPLACE renames a temporary table onto the target name; TimeSeries does
# not support rename, so the statement must be rejected. Before the fix the rejection
# came too late and left the outer table detached without adjusting the digest.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --allow_experimental_time_series_table=1 \
    -q "CREATE OR REPLACE TABLE ${DB}.ts ENGINE = TimeSeries" 2>&1 \
    | grep -o "Renaming is not supported by storage TimeSeries yet" | head -1

# A plain RENAME must be rejected the same way.
${CLICKHOUSE_CLIENT} \
    -q "RENAME TABLE ${DB}.ts TO ${DB}.ts2" 2>&1 \
    | grep -o "Renaming is not supported by storage TimeSeries yet" | head -1

# The outer table must still be present after the rejected operations.
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.ts"

# Force the metadata digest assertion to always run (skip the 1/16 probability gate).
# In release builds assertDigestWithProbability is compiled out, so this is a no-op
# there; the subsequent DDL succeeds regardless.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_replicated_force_metadata_digest_check"

# WITHOUT the fix (debug/sanitizer builds): the digest is inconsistent and this DDL
# aborts the server with "Digest does not match".
# WITH the fix: the rename was rejected cleanly, the digest is consistent, DDL succeeds.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}.probe (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id" 2>&1 \
    | grep -o "LOGICAL_ERROR" | head -1

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_replicated_force_metadata_digest_check"

echo "OK"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB} SYNC" 2>/dev/null || true
