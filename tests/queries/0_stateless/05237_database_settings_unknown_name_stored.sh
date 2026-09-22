#!/usr/bin/env bash
# A `SETTINGS` name that is not a setting at all is refused when a database definition is stated, but a
# database whose definition was stored before that check existed still has to load: refusing it fails the
# whole startup metadata load rather than the one database. A full-definition `ATTACH DATABASE db UUID
# '...' ENGINE = ...` states its settings itself, so it is checked the way `CREATE` is, and a backup
# stores a `CREATE DATABASE`, so a restore has to be recognised as a replay as well.
#
# `clickhouse-local` over a prepared data directory is how the stored metadata is obtained here: the
# database is created with a real setting, its stored definition is then edited into the form a server
# without this check would have written, and the next start loads it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/backups"

CONFIG="${WORK_DIR}/config.xml"
cat > "${CONFIG}" <<EOF
<clickhouse>
    <backups>
        <allowed_path>${WORK_DIR}/backups</allowed_path>
    </backups>
</clickhouse>
EOF

echo '--- a full-definition ATTACH states its settings, so they are checked ---'
# A literal UUID would collide between parallel runs, since it is server-global.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "
ATTACH DATABASE ${CLICKHOUSE_DATABASE}_attach UUID '${uuid}' ENGINE = Atomic
SETTINGS not_a_setting_at_all = DEFAULT;" 2>&1 >/dev/null | grep -o -m 1 -F 'UNKNOWN_SETTING'

echo '--- a stored definition naming a non-setting still loads ---'
# `lazy_load_tables` is a setting of the engine, so its reset form stays in the clause and is persisted
# verbatim, which is what gives a real stored clause to rename.
$CLICKHOUSE_LOCAL --path "${WORK_DIR}/data" -q "
CREATE DATABASE db ENGINE = Atomic SETTINGS max_tables = 10, lazy_load_tables = DEFAULT;
CREATE TABLE db.t (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO db.t VALUES (7);
"

metadata_file="${WORK_DIR}/data/metadata/db.sql"
sed -i 's/lazy_load_tables/not_a_setting_at_all/' "${metadata_file}"
# Without this the arm would pass on an unmodified definition, i.e. assert nothing.
grep -c -m 1 -F 'not_a_setting_at_all' "${metadata_file}"

$CLICKHOUSE_LOCAL --path "${WORK_DIR}/data" -q "
SELECT * FROM db.t;
SELECT extract(engine_full, 'not_a_setting_at_all') FROM system.databases WHERE name = 'db';
"

echo '--- a RESTORE of such a definition still succeeds ---'
# The backup stores a `CREATE DATABASE`, so the restore carries neither the short-ATTACH nor the
# metadata-replay marker that exempt the two arms above.
$CLICKHOUSE_LOCAL --config-file "${CONFIG}" --path "${WORK_DIR}/data" -q "
BACKUP DATABASE db TO File('${WORK_DIR}/backups/b1') FORMAT Null;
RESTORE DATABASE db AS db_restored FROM File('${WORK_DIR}/backups/b1') FORMAT Null;
SELECT * FROM db_restored.t;
SELECT extract(engine_full, 'not_a_setting_at_all') FROM system.databases WHERE name = 'db_restored';
"
