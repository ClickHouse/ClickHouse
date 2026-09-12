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
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS smd_tbl SYNC"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE smd_tbl (a UInt64)
ENGINE = S3Queue('http://localhost:1/bucketname/data/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'unordered', keeper_path = '${KEEPER_PATH}', loading_retries = 42"

echo "-- settings the shared metadata is authoritative for"
# `loading_retries` is named in the definition above and still reports `shared_metadata`: the value a
# replica uses comes from Keeper, so saying `definition` would name a source the engine does not
# consult. This is the one place a source outranks the table's own clause.
# `keeper_path` is deliberately not printed with its value: it carries this test's database name,
# which differs on every run, so the value cannot go into a reference.
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'smd_tbl'
  AND name IN ('mode', 'loading_retries', 'after_processing')
ORDER BY name"

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

$CLICKHOUSE_CLIENT -q "DROP TABLE smd_tbl SYNC"
