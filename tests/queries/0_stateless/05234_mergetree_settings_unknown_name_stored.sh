#!/usr/bin/env bash
# A `SETTINGS` name that is not a setting at all is refused when a MergeTree definition is stated, but
# a table whose definition was stored before that check existed still has to load: refusing it while
# the metadata is read fails the whole load rather than the one table. This test covers what such a
# table can still do - load, be read, be altered, and back a refreshable view - and the one route that
# is refused again because it restates the stored clause as a new definition.
#
# `clickhouse-local` over a prepared data directory is how the stored metadata is obtained here: the
# table is created with a real setting, its stored definition is then edited into the form a server
# without this check would have written, and the next start loads it. No SQL statement can plant such
# a definition on a server that already has the check, which is why this is not a `.sql` test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

echo '--- a stored definition naming a non-setting still loads ---'
# `min_bytes_for_wide_part` is a setting of the engine, so its reset form stays in the clause and is
# persisted verbatim - which is what gives a real stored clause to rename.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.t (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 4096, min_bytes_for_wide_part = DEFAULT;
INSERT INTO db.t VALUES (7);
"

metadata_file=$(grep -rl 'min_bytes_for_wide_part' "${WORKING_DIR}" --include='*.sql')
sed -i 's/min_bytes_for_wide_part/not_a_setting_at_all/' "${metadata_file}"
# Without this the arm would pass on an unmodified definition, i.e. assert nothing.
grep -c -m 1 -F 'not_a_setting_at_all' "${metadata_file}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
SELECT * FROM db.t;
SELECT extract(create_table_query, 'not_a_setting_at_all') FROM system.tables WHERE database = 'db' AND name = 't';
"

echo '--- an unrelated ALTER on such a table still works ---'
# The stale name has to survive the metadata round trip an ALTER performs, or the table would be
# stranded at its first ALTER rather than at load.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
ALTER TABLE db.t MODIFY SETTING merge_with_ttl_timeout = 100;
SELECT extract(create_table_query, 'merge_with_ttl_timeout = 100') FROM system.tables WHERE database = 'db' AND name = 't';
SELECT extract(create_table_query, 'not_a_setting_at_all') FROM system.tables WHERE database = 'db' AND name = 't';
"

echo '--- a refreshable view re-creates its target from a stored definition, so it keeps refreshing ---'
# A non-append refresh re-issues the inner table's stored definition as a plain `CREATE`, which is the
# one load-time route that would otherwise be judged as fresh input - and it would fail as a recorded
# refresh failure rather than as a query error.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE TABLE db.src (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO db.src VALUES (7);
CREATE MATERIALIZED VIEW db.mv REFRESH EVERY 1 YEAR (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = DEFAULT AS SELECT x FROM db.src;
SYSTEM WAIT VIEW db.mv;
"

inner_metadata_file=$(grep -rl 'min_bytes_for_wide_part' "${WORKING_DIR}/store" --include='*inner_id*.sql')
sed -i 's/min_bytes_for_wide_part/not_a_setting_at_all/' "${inner_metadata_file}"
# Without this the arm would pass on an unmodified definition, i.e. assert nothing.
grep -c -m 1 -F 'not_a_setting_at_all' "${inner_metadata_file}"

# The new row is what proves the refresh ran: `SYSTEM WAIT VIEW` alone also returns for a refresh that
# never started, and it raises REFRESH_FAILED when one fails.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
INSERT INTO db.src VALUES (8);
SYSTEM REFRESH VIEW db.mv;
SYSTEM WAIT VIEW db.mv;
SELECT count() FROM db.mv;
"

echo '--- a stored non-setting is inherited by CREATE TABLE AS, which is fresh input ---'
# The stored clause is copied wholesale into the new definition, so it is stated rather than loaded.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" --send_logs_level fatal -q "
CREATE TABLE db.t_copy AS db.t;" 2>&1 >/dev/null | grep -o -m 1 -F 'UNKNOWN_SETTING'

rm -rf "${WORKING_DIR}"
