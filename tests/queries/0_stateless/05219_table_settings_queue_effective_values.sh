#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Tag justification: needs the S3Queue engine, which is an optional build, and Keeper, which holds the
# table metadata this engine reconstructs its settings from.
#
# `S3Queue` and `AzureQueue` keep no settings object: `StorageObjectStorageQueue::getSettings` rebuilds one
# from the table metadata in Keeper, the metadata handle and the storage's own members. Everything it does
# not assign - the format settings the struct carries, which are most of it - stays at a compiled-in default
# in that rebuilt object, while the table really does use what its `SETTINGS` clause said. So for those the
# definition is the only truthful source of the value, and reporting the default would claim the table
# ignores a setting it honours.
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
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS qfv SYNC;
DROP TABLE IF EXISTS qfv_legacy SYNC;
DROP TABLE IF EXISTS qfv_joined SYNC"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE qfv (a UInt64)
ENGINE = S3Queue('http://localhost:1/bucketname/data/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'unordered', keeper_path = '${KEEPER_PATH}',
         input_format_tsv_skip_first_lines = 2, input_format_allow_errors_num = 5,
         polling_min_timeout_ms = 1234, loading_retries = 42"

echo "-- a format setting the definition states reports the value the table uses, not its default"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, \`default\`, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'qfv'
  AND name IN ('input_format_tsv_skip_first_lines', 'input_format_allow_errors_num')
ORDER BY name"

echo "-- and so does one the definition spells the way this engine used to accept it"
# `loadFromQuery` rewrites the `s3queue_` prefix rather than declaring an alias, so attribution and the
# value have to read the legacy spelling the same way the engine does.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE qfv_legacy (a UInt64)
ENGINE = S3Queue('http://localhost:1/bucketname/data/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'unordered', keeper_path = '${KEEPER_PATH}_legacy',
         s3queue_input_format_tsv_skip_first_lines = 3"

$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'qfv_legacy' AND name = 'input_format_tsv_skip_first_lines'"

echo "-- a setting the storage keeps as a member of its own is not taken from the definition instead"
$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'qfv' AND name = 'polling_min_timeout_ms'"

echo "-- and the shared metadata still outranks a definition that states otherwise"
# The second table joins the queue above, whose metadata already holds `loading_retries = 42`, and states 7
# itself. Reporting 7 here would mean the definition had been allowed to win over Keeper.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE qfv_joined (a UInt64)
ENGINE = S3Queue('http://localhost:1/bucketname/data/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'unordered', keeper_path = '${KEEPER_PATH}', loading_retries = 7"

$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'qfv_joined' AND name = 'loading_retries'"

$CLICKHOUSE_CLIENT -q "
DROP TABLE qfv_joined SYNC;
DROP TABLE qfv_legacy SYNC;
DROP TABLE qfv SYNC"
