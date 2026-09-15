#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification: needs the S3Queue engine, which is an optional build.
#
# `shared_metadata` is the one source in `system.table_settings` that outranks the table's own
# `SETTINGS` clause. `S3Queue` and `AzureQueue` keep the settings that must agree between replicas in
# ClickHouse Keeper, and `StorageObjectStorageQueue::getSettings` rebuilds them from there, so what
# the table uses is what Keeper says even when its `CREATE` query states something else - which is
# exactly the case an `ALTER ... MODIFY SETTING` run on another replica produces.
#
# The bucket is never read: the settings a queue table reports do not depend on the object storage
# being reachable, so this needs no S3 endpoint.
#
# A shell test rather than a `.sql` one because `keeper_path` is server-wide, so it has to carry this
# test's database name or two parallel runs collide on the same Keeper node.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KEEPER_PATH="/clickhouse/test_05140_${CLICKHOUSE_DATABASE}"

# Re-runnable: the flaky check runs a new test many times against the same database, and a Keeper
# node outlives the table that created it.
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS smd_tbl SYNC;
DROP TABLE IF EXISTS smd_tbl_joined SYNC;
DROP TABLE IF EXISTS smd_hive_by_mode SYNC;
DROP TABLE IF EXISTS smd_hive_by_flag SYNC"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE smd_tbl (a UInt64)
ENGINE = S3Queue('http://localhost:1/bucketname/data/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'unordered', keeper_path = '${KEEPER_PATH}', loading_retries = 42"

echo "-- settings the shared metadata is authoritative for"
# `loading_retries` is named in the definition above and still reports `shared_metadata`: the value a
# replica uses comes from Keeper, so saying `definition` would name a source the engine does not
# consult. This is the one place a source outranks the table's own clause.
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'smd_tbl'
  AND name IN ('mode', 'loading_retries', 'after_processing')
ORDER BY name"

echo "-- keeper_path names where the shared metadata lives, but is not part of it"
# The table metadata in Keeper does not store it and no `ALTER` can change it: the storage keeps the
# path it was created with, so the definition is its source. Not printed with its value, which carries
# this test's database name and so differs on every run.
$CLICKHOUSE_CLIENT -q "
SELECT name, value = '${KEEPER_PATH}' AS value_is_the_one_asked_for, source
FROM system.table_settings
WHERE database = currentDatabase() AND table = 'smd_tbl' AND name = 'keeper_path'"

echo "-- a setting the queue does not keep there is still attributed normally"
$CLICKHOUSE_CLIENT -q "
SELECT name, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'smd_tbl'
  AND name IN ('polling_min_timeout_ms', 'parallel_inserts')
ORDER BY name"

echo "-- every source this table reports"
$CLICKHOUSE_CLIENT -q "
SELECT DISTINCT source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'smd_tbl'
ORDER BY source"

echo "-- the shared metadata wins over a definition that states otherwise"
# A second table on the same `keeper_path` joins the queue above, whose metadata in Keeper already holds
# `loading_retries = 42`. Its own definition states 7 - what a replica's definition still says after an
# `ALTER` on another replica - and the table reports the value it uses. The same definition spells another
# setting the legacy way, with the `s3queue_` prefix, which is still attributed to the definition.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE smd_tbl_joined (a UInt64)
ENGINE = S3Queue('http://localhost:1/bucketname/data/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'unordered', keeper_path = '${KEEPER_PATH}', loading_retries = 7, s3queue_polling_min_timeout_ms = 1234"

$CLICKHOUSE_CLIENT -q "
SELECT create_table_query LIKE '%loading_retries = 7%' AS definition_states_7 FROM system.tables
WHERE database = currentDatabase() AND name = 'smd_tbl_joined'"

$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'smd_tbl_joined'
  AND name IN ('loading_retries', 'polling_min_timeout_ms')
ORDER BY name"

echo "-- use_hive_partitioning reports the source of partitioning_mode, into which it is folded"
# Whichever of the two the definition states, the table metadata keeps only `partitioning_mode`.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE smd_hive_by_mode (a UInt64, date String)
ENGINE = S3Queue('http://localhost:1/bucketname/data/date=*/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'ordered', keeper_path = '${KEEPER_PATH}_hive_by_mode', partitioning_mode = 'hive'"

$CLICKHOUSE_CLIENT --allow_experimental_object_storage_queue_hive_partitioning 1 -q "
CREATE TABLE smd_hive_by_flag (a UInt64, date String)
ENGINE = S3Queue('http://localhost:1/bucketname/data/date=*/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'ordered', keeper_path = '${KEEPER_PATH}_hive_by_flag', use_hive_partitioning = 1"

$CLICKHOUSE_CLIENT -q "
SELECT table, name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table IN ('smd_hive_by_mode', 'smd_hive_by_flag')
  AND name IN ('partitioning_mode', 'use_hive_partitioning')
ORDER BY table, name"

$CLICKHOUSE_CLIENT -q "
DROP TABLE smd_hive_by_flag SYNC;
DROP TABLE smd_hive_by_mode SYNC;
DROP TABLE smd_tbl_joined SYNC;
DROP TABLE smd_tbl SYNC"
