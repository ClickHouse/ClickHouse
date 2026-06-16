#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-fasttest
# Tag no-parallel: enables a REGULAR failpoint that affects the whole server process.
# Tag no-replicated-database: the test creates its own Replicated database and the
#                             failpoint is enabled only on one server node.
# Tag no-fasttest: creates a TimeSeries table (experimental engine, disabled in fast test).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_repl"
ZK_PATH="/test/timeseries_rename_digest/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${DB}
    ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')
"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --allow_experimental_time_series_table=1 \
    -q "CREATE TABLE ${DB}.ts ENGINE = TimeSeries"

# Force the metadata digest assertion to always run (skip the 1/16 probability gate), so any
# divergence between the in-memory tables map and ZooKeeper is caught immediately. In release
# builds assertDigestWithProbability is compiled out, so this is a no-op there.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_replicated_force_metadata_digest_check"

# RENAME TABLE must succeed and the table must keep working. Before the fix it was rejected from
# renameInMemory() only after DatabaseAtomic::renameTable() had detached the table and committed
# the ZooKeeper transaction, leaving it counted in tables_metadata_digest but missing locally, so
# the next forced digest assertion aborted the server with "Digest does not match".
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "RENAME TABLE ${DB}.ts TO ${DB}.ts2"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.ts"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.ts2"

# RENAME DATABASE must succeed too (it renames every contained table in place).
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "RENAME DATABASE ${DB} TO ${DB}_renamed"
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${DB}"
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${DB}_renamed"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}_renamed.ts2"

# CREATE OR REPLACE creates a temporary table and renames it onto the target name; that internal
# rename is the path the original report tripped over. With the target absent it must create the
# table cleanly.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --allow_experimental_time_series_table=1 \
    -q "CREATE OR REPLACE TABLE ${DB}_renamed.cor ENGINE = TimeSeries"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}_renamed.cor"

# With the forced digest check still on, an unrelated DDL must SUCCEED, proving the digest stayed
# consistent through every rename above. Run it without grep so a crash, lost connection, or any
# other failure fails the test instead of being masked.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
    -q "CREATE TABLE ${DB}_renamed.probe (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}_renamed.probe"

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_replicated_force_metadata_digest_check"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_renamed SYNC" 2>/dev/null || true
