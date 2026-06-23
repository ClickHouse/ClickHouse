#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database
# Tag no-parallel: enables a REGULAR failpoint that affects the whole server process.
# Tag no-replicated-database: the test creates its own Replicated database, and the experimental
#                             TimeSeries engine does not round-trip through a Replicated default database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RENAME DATABASE moves every table of the database together. A materialized view's external "TO"
# target and a TimeSeries table's external SAMPLES/TAGS/METRICS targets are stored with the database
# name; they must be rewritten to the new database name (in memory and on disk) so the tables stay
# usable after the rename and survive a reload.

###########################################################################################
# Replicated database: materialized view with an external TO target. This also exercises the
# DatabaseReplicated path that rewrites the metadata node and the digest in ZooKeeper.
###########################################################################################
RDB_OLD="${CLICKHOUSE_DATABASE}_rold"
RDB_NEW="${CLICKHOUSE_DATABASE}_rnew"
ZK_PATH="/test/rename_database_view_targets/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${RDB_OLD} ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${RDB_OLD}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${RDB_OLD}.dest (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW ${RDB_OLD}.mv TO ${RDB_OLD}.dest AS SELECT x FROM ${RDB_OLD}.src;
    INSERT INTO ${RDB_OLD}.dest VALUES (10), (20);
"

# Force the metadata digest assertion to always run (skip the 1/16 probability gate), so any
# divergence between the local metadata, ZooKeeper and the digest is caught immediately. In release
# builds assertDigestWithProbability is compiled out, so this is a no-op there.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_replicated_force_metadata_digest_check"

# --send_logs_level=fatal hides the benign "Replacing outdated dependencies" notice: the view's TO
# target dependency moves to the new database while its SELECT source stays in the old name.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --send_logs_level=fatal -q "RENAME DATABASE ${RDB_OLD} TO ${RDB_NEW}"

echo "-- Replicated: materialized view external TO target --"
# Selecting from a "TO" materialized view reads from its target table, so this resolves the in-memory
# target id. Before the fix it pointed at the dropped old database and threw UNKNOWN_TABLE.
echo "mv reads target after rename:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${RDB_NEW}.mv"

# Reload the database from local metadata. If the TO target had not been rewritten, the reloaded view
# would point at the dropped old database and the read below would fail. (Only the external TO target
# is rewritten; the view's SELECT source is intentionally left untouched, as for any RENAME DATABASE.)
${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "DETACH DATABASE ${RDB_NEW}; ATTACH DATABASE ${RDB_NEW}"
echo "mv reads target after reload:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${RDB_NEW}.mv"

# With the forced digest check still on, an unrelated DDL must SUCCEED, proving the digest stayed
# consistent through the rewrite. Run it without grep so a crash or lost connection fails the test.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "CREATE TABLE ${RDB_NEW}.probe (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"
echo "digest consistent (probe table exists):"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${RDB_NEW}.probe"

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_replicated_force_metadata_digest_check"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${RDB_NEW} SYNC" 2>/dev/null || true

###########################################################################################
# Atomic database: TimeSeries table with external SAMPLES/TAGS/METRICS targets (the reported case).
###########################################################################################
ADB_OLD="${CLICKHOUSE_DATABASE}_aold"
ADB_NEW="${CLICKHOUSE_DATABASE}_anew"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${ADB_OLD} ENGINE = Atomic"

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ${ADB_OLD}.smpl (id UInt64, timestamp DateTime64(3), value Float64)
        ENGINE = MergeTree ORDER BY (id, timestamp);
    CREATE TABLE ${ADB_OLD}.tgs (id UInt64, metric_name LowCardinality(String),
        tags Map(LowCardinality(String), String), min_time DateTime64(3), max_time DateTime64(3))
        ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${ADB_OLD}.mtr (metric_family_name String, type String, unit String, help String)
        ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
    CREATE TABLE ${ADB_OLD}.ts ENGINE = TimeSeries
        SAMPLES ${ADB_OLD}.smpl TAGS ${ADB_OLD}.tgs METRICS ${ADB_OLD}.mtr;
    INSERT INTO ${ADB_OLD}.smpl VALUES (1, toDateTime64(100, 3), 10.0), (2, toDateTime64(100, 3), 20.0);
"

${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "RENAME DATABASE ${ADB_OLD} TO ${ADB_NEW}"

echo "-- Atomic: TimeSeries external targets --"
# Inserting through the TimeSeries table routes to its SAMPLES target, resolving the in-memory target
# id. Before the fix it pointed at the dropped old database and threw UNKNOWN_TABLE.
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    INSERT INTO ${ADB_NEW}.ts (metric_name, tags, time_series)
    VALUES ('foo', map(), [(toDateTime64(200, 3), 5.0)])"
echo "samples target got the row routed through TimeSeries:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${ADB_NEW}.smpl"
echo "targets rewritten on disk (occurrences of old db name, want 0):"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${ADB_NEW}.ts" | grep -o -F "${ADB_OLD}" | wc -l

# Reload from on-disk metadata: the TimeSeries table is rebuilt from its rewritten .sql definition.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "DETACH DATABASE ${ADB_NEW}; ATTACH DATABASE ${ADB_NEW}"
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    INSERT INTO ${ADB_NEW}.ts (metric_name, tags, time_series)
    VALUES ('bar', map(), [(toDateTime64(300, 3), 7.0)])"
echo "samples target still reachable after reload:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${ADB_NEW}.smpl"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${ADB_NEW} SYNC" 2>/dev/null || true
