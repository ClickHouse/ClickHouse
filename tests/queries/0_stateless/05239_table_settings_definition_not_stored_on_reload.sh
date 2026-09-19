#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on the `MySQL` table engine, which is not built in fast test.
#
# A loader may add a setting to the definition it loads. Where that definition is stored - `CREATE`, a full
# `ATTACH` - the setting becomes the definition's, but where the table is loaded from what is already stored and
# nothing is written back - a restart, a short `ATTACH` - the addition stays in memory, and `system.table_settings`
# must not report as the definition's what the stored `CREATE` query does not state.
#
# `MergeTree` adds `index_granularity` to a clause that leaves it out. A definition stored before it did so is made
# here by removing it from the metadata file of a `clickhouse-local` database and loading that again.
#
# A `MySQL` table bridges the session's `mysql_datatypes_support_level` into its definition. A short `ATTACH` does
# not store the definition again, so it does not bridge: the value stays the stored one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_PATH="${CLICKHOUSE_TMP}/05239_local_data"
rm -rf "${DATA_PATH}"

$CLICKHOUSE_LOCAL --path "${DATA_PATH}" --query "
CREATE DATABASE db;
CREATE TABLE db.mt (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS merge_max_block_size = 100;"

METADATA_FILE="${DATA_PATH}/metadata/db/mt.sql"
sed -i 's/, index_granularity = [0-9]*//' "${METADATA_FILE}"

echo "-- MergeTree loaded from a definition that does not state index_granularity"
$CLICKHOUSE_LOCAL --path "${DATA_PATH}" --query "
SELECT position(create_table_query, 'index_granularity') > 0 AS stated FROM system.tables WHERE database = 'db' AND name = 'mt';
SELECT name, source = 'definition' AS stated, source IN ('default', 'config') AS unstated FROM system.table_settings
WHERE database = 'db' AND table = 'mt' AND name IN ('index_granularity', 'merge_max_block_size')
ORDER BY name;"

rm -rf "${DATA_PATH}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mysql_short_attach"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mysql_short_attach (x Int32) ENGINE = MySQL('unreachable.invalid:3306', 'db', 'tbl', 'user', 'password')"
$CLICKHOUSE_CLIENT -q "DETACH TABLE mysql_short_attach"
$CLICKHOUSE_CLIENT --mysql_datatypes_support_level decimal -q "ATTACH TABLE mysql_short_attach"

echo "-- MySQL after a short ATTACH in a session with another mysql_datatypes_support_level"
$CLICKHOUSE_CLIENT -q "
SELECT name, value = \`default\` AS is_default, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'mysql_short_attach' AND name = 'mysql_datatypes_support_level'"

$CLICKHOUSE_CLIENT -q "DROP TABLE mysql_short_attach"
