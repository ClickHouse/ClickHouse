#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel-replicas
# Tag no-replicated-database: RENAME of multiple tables in a single query is not supported there,
#                             and database renames are handled differently.
# Tag no-parallel-replicas: this test only checks row-policy bindings, parallel replicas are irrelevant here.

# Row policies are keyed by (database, table). They must follow the table on RENAME TABLE,
# EXCHANGE TABLES and RENAME DATABASE, otherwise the policy is orphaned on the old name and
# the table becomes readable with no filtering under its new name (a row-policy escape).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_${CLICKHOUSE_DATABASE}"
DB2="${CLICKHOUSE_DATABASE}_2"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${USER}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, CREATE DATABASE, DROP DATABASE ON *.* TO ${USER}"

run_user() { ${CLICKHOUSE_CLIENT} --user "${USER}" --query "$1"; }

echo '-- RENAME TABLE: policy follows the table'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.data (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.data VALUES (1, 'eng'), (2, 'fin'), (3, 'eng')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY rp ON ${CLICKHOUSE_DATABASE}.data FOR SELECT USING dept = 'eng' TO ${USER}"
echo 'before (eng only):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.data ORDER BY id"
run_user "RENAME TABLE ${CLICKHOUSE_DATABASE}.data TO ${CLICKHOUSE_DATABASE}.data2"
echo 'after rename (still eng only, not the fin row):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.data2 ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT short_name, table FROM system.row_policies WHERE short_name = 'rp' AND database = '${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY rp ON ${CLICKHOUSE_DATABASE}.data2"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.data2"

echo '-- EXCHANGE TABLES: each policy follows its data (so protection is preserved)'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.ea (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.eb (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.ea VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.eb VALUES (10, 'eng'), (20, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY pa ON ${CLICKHOUSE_DATABASE}.ea FOR SELECT USING dept = 'eng' TO ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY pb ON ${CLICKHOUSE_DATABASE}.eb FOR SELECT USING dept = 'fin' TO ${USER}"
echo 'before: ea has {1,2} with eng policy -> 1; eb has {10,20} with fin policy -> 20:'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.ea ORDER BY id"
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.eb ORDER BY id"
run_user "EXCHANGE TABLES ${CLICKHOUSE_DATABASE}.ea AND ${CLICKHOUSE_DATABASE}.eb"
echo 'after exchange the policies follow their data: name ea now holds {10,20} guarded by the fin policy -> 20; name eb now holds {1,2} guarded by the eng policy -> 1:'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.ea ORDER BY id"
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.eb ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY pa ON ${CLICKHOUSE_DATABASE}.eb"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY pb ON ${CLICKHOUSE_DATABASE}.ea"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.ea"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.eb"

echo '-- EXCHANGE TABLES with the same policy short name on both tables (no transient name collision)'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.sa (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.sb (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.sa VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.sb VALUES (10, 'eng'), (20, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY samename ON ${CLICKHOUSE_DATABASE}.sa FOR SELECT USING dept = 'eng' TO ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY samename ON ${CLICKHOUSE_DATABASE}.sb FOR SELECT USING dept = 'fin' TO ${USER}"
run_user "EXCHANGE TABLES ${CLICKHOUSE_DATABASE}.sa AND ${CLICKHOUSE_DATABASE}.sb"
echo 'after exchange the policies follow their data: name sa holds {10,20} guarded by fin -> 20; name sb holds {1,2} guarded by eng -> 1:'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.sa ORDER BY id"
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.sb ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY samename ON ${CLICKHOUSE_DATABASE}.sa"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY samename ON ${CLICKHOUSE_DATABASE}.sb"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.sa"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.sb"

echo '-- cross-database RENAME: policy follows to the new database and table'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB2}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB2}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.ca (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.ca VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY cp ON ${CLICKHOUSE_DATABASE}.ca FOR SELECT USING dept = 'eng' TO ${USER}"
run_user "RENAME TABLE ${CLICKHOUSE_DATABASE}.ca TO ${DB2}.cb"
echo 'after cross-db rename (eng only):'
run_user "SELECT id FROM ${DB2}.cb ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT database, table FROM system.row_policies WHERE short_name = 'cp' AND database = '${DB2}'"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY cp ON ${DB2}.cb"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${DB2}.cb"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB2}"

echo '-- RENAME DATABASE: database-wide and per-table policies follow'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB2}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_src"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_src.t (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}_src.t VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY dbp ON ${CLICKHOUSE_DATABASE}_src.* FOR SELECT USING dept = 'eng' TO ${USER}"
run_user "RENAME DATABASE ${CLICKHOUSE_DATABASE}_src TO ${DB2}"
echo 'after database rename (db-wide policy still applies -> eng only):'
run_user "SELECT id FROM ${DB2}.t ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT database FROM system.row_policies WHERE short_name = 'dbp' AND database = '${DB2}'"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY dbp ON ${DB2}.*"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB2}"

echo '-- failed RENAME: policy binding is preserved'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.ra (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.ra VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.rexist (id UInt64) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY rpol ON ${CLICKHOUSE_DATABASE}.ra FOR SELECT USING dept = 'eng' TO ${USER}"
run_user "RENAME TABLE ${CLICKHOUSE_DATABASE}.ra TO ${CLICKHOUSE_DATABASE}.rexist" 2>&1 | grep -o -m1 "TABLE_ALREADY_EXISTS"
echo 'after failed rename (policy stays, eng only):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.ra ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT table FROM system.row_policies WHERE short_name = 'rpol' AND database = '${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY rpol ON ${CLICKHOUSE_DATABASE}.ra"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.ra"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.rexist"

# The re-key cannot be applied atomically with the table rename, so it must be checked BEFORE the
# rename commits: otherwise a re-key that throws after the commit leaves the renamed table readable
# without its filter (the very escape this fixes). The next two cases are the unmovable-policy paths.

echo '-- RENAME rejected when destination row-policy name is taken (no commit-then-leak)'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.ka (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.ka VALUES (1, 'eng'), (2, 'fin')"
# A stationary policy already occupies the destination name 'kp ON kc'; the moving 'kp ON ka' cannot land there.
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY kp ON ${CLICKHOUSE_DATABASE}.ka FOR SELECT USING dept = 'eng' TO ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY kp ON ${CLICKHOUSE_DATABASE}.kc FOR SELECT USING 1 TO ${USER}"
run_user "RENAME TABLE ${CLICKHOUSE_DATABASE}.ka TO ${CLICKHOUSE_DATABASE}.kc" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
echo 'after rejected rename (table not renamed, policy stays on ka, eng only):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.ka ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'kc'"
${CLICKHOUSE_CLIENT} --query "SELECT table FROM system.row_policies WHERE short_name = 'kp' AND database = '${CLICKHOUSE_DATABASE}' ORDER BY table"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY kp ON ${CLICKHOUSE_DATABASE}.ka, kp ON ${CLICKHOUSE_DATABASE}.kc"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.ka"

echo '-- RENAME rejected when the policy is in a read-only storage (users.xml), via clickhouse-local'
LOCAL_DIR="${CLICKHOUSE_TMP}/04401_local"
rm -rf "${LOCAL_DIR}"
mkdir -p "${LOCAL_DIR}"
cat > "${LOCAL_DIR}/users.xml" <<'XML'
<clickhouse>
    <users><default>
        <password></password><profile>default</profile><quota>default</quota>
        <access_management>1</access_management>
        <databases><rodb><rt><filter>dept = 'eng'</filter></rt></rodb></databases>
    </default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
XML
cat > "${LOCAL_DIR}/config.xml" <<'XML'
<clickhouse>
    <user_directories><users_xml><path>users.xml</path></users_xml></user_directories>
</clickhouse>
XML
${CLICKHOUSE_LOCAL} --config-file "${LOCAL_DIR}/config.xml" --path "${LOCAL_DIR}" -q "
CREATE DATABASE rodb;
CREATE TABLE rodb.rt (id UInt64, dept String) ENGINE = MergeTree ORDER BY id;
INSERT INTO rodb.rt VALUES (1, 'eng'), (2, 'fin');
RENAME TABLE rodb.rt TO rodb.rt2;
" 2>&1 | grep -o -m1 "ACCESS_STORAGE_READONLY"
echo 'after rejected rename (table not renamed, read-only policy still bound to rt):'
${CLICKHOUSE_LOCAL} --config-file "${LOCAL_DIR}/config.xml" --path "${LOCAL_DIR}" -q "
SELECT count() FROM system.tables WHERE database = 'rodb' AND name = 'rt2';
SELECT table FROM system.row_policies WHERE database = 'rodb';
"
rm -rf "${LOCAL_DIR}"

# A database-wide policy (ON db.*) is not bound to a single table name, so it cannot follow a table
# that moves to a different database (the destination lookup new_db.t / new_db.* never sees old db.*).
# Reject the cross-database move rather than silently dropping the filter.
echo '-- cross-database RENAME rejected when a database-wide (db.*) policy applies'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB2}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_x"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB2}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_x.t (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}_x.t VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY xp ON ${CLICKHOUSE_DATABASE}_x.* FOR SELECT USING dept = 'eng' TO ${USER}"
run_user "RENAME TABLE ${CLICKHOUSE_DATABASE}_x.t TO ${DB2}.t2" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
echo 'after rejected cross-db rename (table not moved, db-wide policy still filters source -> eng only):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}_x.t ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${DB2}' AND name = 't2'"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY xp ON ${CLICKHOUSE_DATABASE}_x.*"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE}_x"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB2}"

# The re-key parks each policy under a transient '.tmp_rename_row_policy_<uuid>_0' name during the
# move. That name is derived from the visible policy UUID, so a pre-existing policy can occupy it
# deterministically; the move would then throw AFTER the rename commits. Preflight rejects it first.
echo '-- RENAME rejected when the transient (phase-1) row-policy name is taken (no commit-then-leak)'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.ta (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.ta VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY tp ON ${CLICKHOUSE_DATABASE}.ta FOR SELECT USING dept = 'eng' TO ${USER}"
TP_ID=$(${CLICKHOUSE_CLIENT} --query "SELECT id FROM system.row_policies WHERE short_name = 'tp' AND database = '${CLICKHOUSE_DATABASE}' AND table = 'ta'")
TMP_TABLE=".tmp_rename_row_policy_${TP_ID}_0"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY tp ON ${CLICKHOUSE_DATABASE}.\`${TMP_TABLE}\` FOR SELECT USING 1 TO ${USER}"
run_user "RENAME TABLE ${CLICKHOUSE_DATABASE}.ta TO ${CLICKHOUSE_DATABASE}.tb" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
echo 'after rejected rename (table not renamed, policy stays on ta, eng only):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.ta ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'tb'"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY tp ON ${CLICKHOUSE_DATABASE}.ta, tp ON ${CLICKHOUSE_DATABASE}.\`${TMP_TABLE}\`"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.ta"

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER}"
