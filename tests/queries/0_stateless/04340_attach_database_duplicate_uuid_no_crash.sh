#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-ordinary-database, no-replicated-database
# no-parallel: edits on-disk table metadata of its own database and re-reads it via ATTACH DATABASE
# no-ordinary-database / no-replicated-database: relies on the Atomic store/<uuid>/*.sql layout

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Two tables in one Atomic database are forced to share the same table UUID on disk
# (mirrors a leftover _tmp_replace_* table from CREATE OR REPLACE ... UUID '...' that
# carries the same explicit UUID as the live table). On ATTACH DATABASE the tables are
# loaded in parallel and both reach DatabaseCatalog::addUUIDMapping with that UUID.
# This used to abort the server with a LOGICAL_ERROR; it must instead fail the load with
# a regular TABLE_ALREADY_EXISTS error and keep the server alive.

db="db_04340_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${db}"
# lazy_load_tables=0 so both tables are loaded (and reach addUUIDMapping) during ATTACH DATABASE,
# which is the code path that used to crash.
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${db} ENGINE=Atomic SETTINGS lazy_load_tables=0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.keep (x UInt32) ENGINE=MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.dup  (x UInt32) ENGINE=MergeTree ORDER BY x"

keep_uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database='${db}' AND name='keep'")
dup_uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database='${db}' AND name='dup'")
dup_metadata=$($CLICKHOUSE_CLIENT -q "SELECT metadata_path FROM system.tables WHERE database='${db}' AND name='dup'")
data_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'")

$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${db}"

# Rewrite dup's metadata so it declares keep's UUID -> two .sql files in the database share one UUID.
sed -i "s/${dup_uuid}/${keep_uuid}/" "${data_path}${dup_metadata}"

# ATTACH DATABASE must fail with a normal collision error, NOT abort the server.
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${db}" 2>&1 | grep -oF "UUID collision" | head -n 1

# Server must still be alive.
$CLICKHOUSE_CLIENT -q "SELECT 'alive'"

# The failed ATTACH rolls back the registration, so the database no longer exists here.
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${db}"
