#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification: needs the S3Queue engine, which is an optional build.
#
# `S3Queue` and `AzureQueue` keep no settings object: `StorageObjectStorageQueue::getSettings` rebuilds
# one from the table metadata in Keeper, the metadata handle and the storage's own members. Everything it
# does not assign - the format settings the struct carries, which are most of it - stays at a compiled-in
# default in that rebuilt object, while the table really does use what its `SETTINGS` clause said. So for
# those the definition is the only truthful source of the value, and reporting the default would claim the
# table ignores a setting it honours.
#
# The bucket is never read: the settings a queue table reports do not depend on the object storage being
# reachable, so this needs no S3 endpoint.
#
# A shell test rather than a `.sql` one because `keeper_path` is server-wide, so it has to carry this
# test's database name or two parallel runs collide on the same Keeper node.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KEEPER_PATH="/clickhouse/test_05219_${CLICKHOUSE_DATABASE}"

# Re-runnable: the flaky check runs a new test many times against the same database, and a Keeper node
# outlives the table that created it.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS qfv SYNC"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE qfv (a UInt64)
ENGINE = S3Queue('http://localhost:1/bucketname/data/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'unordered', keeper_path = '${KEEPER_PATH}',
         input_format_tsv_skip_first_lines = 2, input_format_allow_errors_num = 5,
         polling_min_timeout_ms = 1234"

echo "-- a format setting the definition states reports the value the table uses, not its default"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, \`default\`, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'qfv'
  AND name IN ('input_format_tsv_skip_first_lines', 'input_format_allow_errors_num')
ORDER BY name"

echo "-- a setting the storage keeps as a member of its own is unaffected"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'qfv' AND name = 'polling_min_timeout_ms'"

echo "-- and one the shared metadata is authoritative for still outranks the definition"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'qfv' AND name = 'mode'"

echo "-- a format setting nothing states reports its default"
$CLICKHOUSE_CLIENT -q "
SELECT name, value = \`default\` AS is_default, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'qfv' AND name = 'input_format_csv_skip_first_lines'"

echo "-- no row claims the default source for a value that is not the default"
$CLICKHOUSE_CLIENT -q "
SELECT count() FROM system.table_settings
WHERE database = currentDatabase() AND table = 'qfv' AND source = 'default' AND value != \`default\`"

$CLICKHOUSE_CLIENT -q "DROP TABLE qfv SYNC"
