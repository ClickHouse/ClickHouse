#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database
# Tag no-parallel: enables a REGULAR failpoint that affects the whole server process.
# Tag no-replicated-database: the test creates its own Replicated database and the
#                             failpoint is enabled only on one server node.

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
# not support rename, so the statement must be rejected.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --allow_experimental_time_series_table=1 \
    -q "CREATE OR REPLACE TABLE ${DB}.ts ENGINE = TimeSeries" 2>&1 \
    | grep -o "Renaming is not supported by storage TimeSeries yet" | head -1

# A plain RENAME TABLE must be rejected the same way.
${CLICKHOUSE_CLIENT} \
    -q "RENAME TABLE ${DB}.ts TO ${DB}.ts2" 2>&1 \
    | grep -o "Renaming is not supported by storage TimeSeries yet" | head -1

# RENAME DATABASE must also be rejected before any metadata/catalog mutation.
${CLICKHOUSE_CLIENT} \
    -q "RENAME DATABASE ${DB} TO ${DB}_renamed" 2>&1 \
    | grep -o "Renaming is not supported by storage TimeSeries yet" | head -1

# The original database must still exist under its old name (RENAME DATABASE must not
# have half-applied: catalog/metadata must not diverge from ZooKeeper).
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${DB}"
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${DB}_renamed"

# The outer table must still be present after the rejected operations.
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.ts"

# Force the metadata digest assertion to always run (skip the 1/16 probability gate).
# In release builds assertDigestWithProbability is compiled out, so this is a no-op
# there; the subsequent DDL succeeds regardless.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_replicated_force_metadata_digest_check"

# The regression contract: with the forced digest check on, the next DDL must SUCCEED.
# Before the fix the digest was inconsistent and this aborted the server with
# "Digest does not match"; assert success directly so a crash, lost connection, or any
# other failure fails the test instead of being masked.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}.probe (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.probe"

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_replicated_force_metadata_digest_check"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB} SYNC" 2>/dev/null || true
