#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database
# Tag no-parallel: enables a REGULAR failpoint that affects the whole server process.
# Tag no-replicated-database: the test creates its own Replicated database and the
#                             failpoint is enabled only on one server node.

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

# In a non-UUID (Ordinary) database the inner tables embed the outer table name, so renaming a
# TimeSeries table renames each inner table too. Those inner renames are not transactional, so if
# one destination name is already taken the rename must be rejected up front, before any inner
# table is moved -- otherwise the table would be left half-renamed with an orphaned inner table.
ORD="${CLICKHOUSE_DATABASE}_ord"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${ORD} SYNC"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --allow_deprecated_database_ordinary=1 -q "CREATE DATABASE ${ORD} ENGINE = Ordinary"
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "CREATE TABLE ${ORD}.ts ENGINE = TimeSeries"
# Occupy one of the destination inner-table names so the rename cannot complete.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${ORD}.\`.inner.tags.ts2\` (x UInt8) ENGINE = MergeTree ORDER BY x"
# The rename is rejected before any inner table is moved.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "RENAME TABLE ${ORD}.ts TO ${ORD}.ts2" 2>&1 | grep -q -F "TABLE_ALREADY_EXISTS" && echo "rejected"
# Every source inner table is still present (none orphaned), so the table keeps working.
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${ORD}.\`.inner.samples.ts\`"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${ORD}.\`.inner.tags.ts\`"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${ORD}.\`.inner.metrics.ts\`"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${ORD}.ts"

${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "DROP DATABASE ${ORD} SYNC" 2>/dev/null || true

# With lazy_load_tables=1 a reloaded TimeSeries table is materialized as a StorageTableProxy. The
# proxy must not hide the TimeSeries type from the cross-database move guard -- otherwise a
# cross-database RENAME would move only the outer table and orphan its inner tables in the old
# database. The outer TimeSeries table is loaded eagerly so the guard sees it.
LZ="${CLICKHOUSE_DATABASE}_lazy"
LZ2="${CLICKHOUSE_DATABASE}_lazy2"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${LZ} SYNC; DROP DATABASE IF EXISTS ${LZ2} SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${LZ} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${LZ2} ENGINE = Atomic"
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "CREATE TABLE ${LZ}.ts ENGINE = TimeSeries"
# Reattach so the table is recreated as a lazy proxy.
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${LZ}; ATTACH DATABASE ${LZ}"
# The cross-database move is rejected before any inner table is moved.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "RENAME TABLE ${LZ}.ts TO ${LZ2}.ts" 2>&1 | grep -q -F "Cannot move TimeSeries table with inner tables" && echo "rejected"
# Every inner table stayed in the source database (none orphaned in the target).
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${LZ}' AND name LIKE '.inner_id.%'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${LZ2}' AND name LIKE '.inner_id.%'"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${LZ}.ts"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "DROP DATABASE ${LZ} SYNC" 2>/dev/null || true
${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "DROP DATABASE ${LZ2} SYNC" 2>/dev/null || true
