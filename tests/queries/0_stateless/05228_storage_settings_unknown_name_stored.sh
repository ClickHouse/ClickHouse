#!/usr/bin/env bash
# A `SETTINGS` name that is not a setting at all is refused when the definition is stated (see
# `05227_storage_settings_unknown_name`), but a table whose definition was stored before that check
# existed still has to load: refusing it while the metadata is read fails the whole load rather than the
# one table. A full-definition `ATTACH TABLE t UUID '...' (...)` states its settings itself, so it is
# checked the way `CREATE` is.
#
# `clickhouse-local` over a prepared data directory is how the stored metadata is obtained here: the
# table is created with a real setting, its stored definition is then edited into the form a server
# without this check would have written, and the next start loads it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

echo '--- a full-definition ATTACH states its settings, so they are checked ---'
# A literal UUID would collide between parallel runs, since it is server-global.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "
ATTACH TABLE t_stored_unknown UUID '${uuid}' (a UInt64) ENGINE = File(CSV)
SETTINGS not_a_setting_at_all = 1;" 2>&1 >/dev/null | grep -o -m 1 -F 'UNKNOWN_SETTING'

echo '--- a stored definition naming a non-setting still loads ---'
# `engine_file_truncate_on_insert` is a query setting and `File` reports the query settings as its own,
# so the split in `InterpreterSetQuery::applySettingsFromQuery` leaves it in the clause and it is
# persisted - which is what gives a real stored clause to rename.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.t (a UInt64) ENGINE = File(CSV) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO db.t VALUES (7);
"

metadata_file=$(grep -rl 'engine_file_truncate_on_insert' "${WORKING_DIR}" --include='*.sql')
sed -i 's/engine_file_truncate_on_insert/not_a_setting_at_all/' "${metadata_file}"
# Without this the arm would pass on an unmodified definition, i.e. assert nothing.
grep -c -m 1 -F 'not_a_setting_at_all' "${metadata_file}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
SELECT * FROM db.t;
SELECT extract(create_table_query, 'not_a_setting_at_all') FROM system.tables WHERE database = 'db' AND name = 't';
"

echo '--- a stored non-setting is inherited by CREATE TABLE AS, which is fresh input ---'
# The stored clause is copied wholesale into the new definition, so it is stated rather than loaded.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" --send_logs_level fatal -q "
CREATE TABLE db.t_copy AS db.t;" 2>&1 >/dev/null | grep -o -m 1 -F 'UNKNOWN_SETTING'

echo '--- a format setting in the clause is applied, not merely accepted ---'
# Read the file back as raw lines: a round trip through the same table would parse with the same
# delimiter it wrote and pass whether or not the setting took effect.
the_file="${WORKING_DIR}/delimiter.csv"
$CLICKHOUSE_LOCAL -q "
CREATE TABLE t_delim (a UInt64, b UInt64) ENGINE = File(CSV, '${the_file}') SETTINGS format_csv_delimiter = '|';
INSERT INTO t_delim VALUES (1, 2);
"
cat "${the_file}"

rm -rf "${WORKING_DIR}"
