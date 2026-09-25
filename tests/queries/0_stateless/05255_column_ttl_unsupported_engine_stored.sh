#!/usr/bin/env bash
# An engine that does not support `TTL` refuses a column `TTL` stated in its definition, but a table
# whose stored definition already carries one still has to load. A definition written without a column
# list takes the source table's columns whole, so a server that still copied a source column's `TTL`
# wrote it back out on the next `ALTER`. Refusing such a definition while the metadata is read fails
# the whole load rather than the one table, and with `async_load_databases = 0` the server does not
# start at all; the only way out is to edit the metadata file by hand. The `TTL` is inert for these
# engines, so it is loaded and ignored rather than acted on.
#
# `clickhouse-local` over a prepared data directory is how the stored metadata is obtained here: the
# table is created without a `TTL`, its stored definition is then edited into the form the older
# server would have written, and the next start loads it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

echo '--- a full-definition ATTACH states its columns, so a column TTL is refused ---'
# A literal UUID would collide between parallel runs, since it is server-global.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "
ATTACH TABLE t_column_ttl_merge UUID '${uuid}' (\`id\` UInt64, \`dt\` Date TTL dt + toIntervalDay(1))
ENGINE = Merge(currentDatabase(), '^no_such_table\$');" 2>&1 >/dev/null | grep -o -m 1 -F 'BAD_ARGUMENTS'

echo '--- a table TTL is refused wherever it is stated, since it is never inherited ---'
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "
CREATE TABLE t_table_ttl_merge (id UInt64, dt Date) ENGINE = Merge(currentDatabase(), '^no_such_table\$')
TTL dt + toIntervalDay(1);" 2>&1 >/dev/null | grep -o -m 1 -F 'BAD_ARGUMENTS'

echo '--- a stored definition carrying an inherited column TTL loads ---'
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.src (id UInt64, dt Date) ENGINE = MergeTree ORDER BY id;
INSERT INTO db.src VALUES (7, '2000-01-01');
CREATE TABLE db.m (id UInt64, dt Date) ENGINE = Merge('db', '^src\$');
"

metadata_file=$(grep -rl 'ENGINE = Merge(' "${WORKING_DIR}" --include='*.sql')
sed -i 's/`dt` Date$/`dt` Date TTL dt + toIntervalDay(1)/' "${metadata_file}"
# Without this the arm would run against an unmodified definition, i.e. assert nothing.
grep -c -m 1 -F 'TTL dt + toIntervalDay(1)' "${metadata_file}"

# The row is read back as it was written: `2000-01-01` is long expired, so a `TTL` that was acted on
# would have reset the column to its default. The table also has to load for the rest of the database
# to load at all.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
SELECT * FROM db.m;
SELECT count() FROM system.tables WHERE database = 'db';
"

echo '--- the stored TTL stays in the definition, so the load is repeatable ---'
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
SELECT extract(create_table_query, 'TTL dt \+ toIntervalDay\(1\)') FROM system.tables WHERE database = 'db' AND name = 'm';
"

rm -rf "${WORKING_DIR}"
