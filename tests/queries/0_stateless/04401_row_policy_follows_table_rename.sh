#!/usr/bin/env bash
# Tags: long, no-replicated-database
# Tag long: many cases (RENAME/EXCHANGE/cross-DB/RENAME DATABASE plus rejection paths, two of which
#           start a separate clickhouse-local server) make a single run heavy; under the flaky-check's
#           repeated parallel runs it exceeds the 180s soft cap. long exempts that cap and runs it ~5x
#           instead of ~50x there, while still running once on every regular and per-test-coverage lane.
# Tag no-replicated-database: RENAME of multiple tables in a single query is not supported there,
#                             and database renames are handled differently.

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
# Needed for the CREATE OR REPLACE / REPLACE TABLE cases below: the default test config enables
# table_engines_require_grant, so specifying ENGINE = MergeTree requires this explicit grant.
${CLICKHOUSE_CLIENT} --query "GRANT TABLE ENGINE ON MergeTree TO ${USER}"

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

# An EXCHANGE moves data in BOTH directions, so both re-keys must also rewrite the DATABASE part of
# the policy name. The same-database exchange case above cannot see a reverse re-key that only fixes
# the table name, because there the database is unchanged either way.
echo '-- cross-database EXCHANGE: each policy follows its data across the database boundary'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB2}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB2}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.xa (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB2}.xb (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.xa VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB2}.xb VALUES (10, 'eng'), (20, 'fin')"
# Different filters, so a re-key that lost the database part (leaving the policy behind or landing it
# on the wrong database) changes which rows the user sees rather than staying invisible.
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY xpa ON ${CLICKHOUSE_DATABASE}.xa FOR SELECT USING dept = 'eng' TO ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY xpb ON ${DB2}.xb FOR SELECT USING dept = 'fin' TO ${USER}"
echo 'before: xa {1,2} with eng policy -> 1; xb {10,20} with fin policy -> 20:'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.xa ORDER BY id"
run_user "SELECT id FROM ${DB2}.xb ORDER BY id"
run_user "EXCHANGE TABLES ${CLICKHOUSE_DATABASE}.xa AND ${DB2}.xb"
echo 'after cross-db exchange the policies followed their data: name xa now holds {10,20} guarded by the fin policy -> 20; name xb now holds {1,2} guarded by the eng policy -> 1:'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.xa ORDER BY id"
run_user "SELECT id FROM ${DB2}.xb ORDER BY id"
echo 'and both bindings moved to the other database:'
# system.row_policies is server-wide, so every read of it has to be scoped to the databases this copy
# of the test owns. The flaky check runs 50 copies of a changed test concurrently and does NOT pass
# --no-self-parallel (only the `targeted` flavor does), so a sibling copy's identically named policy
# would otherwise appear here. The database set is still asserted, because it is printed.
${CLICKHOUSE_CLIENT} --query "SELECT database, table FROM system.row_policies WHERE short_name = 'xpa' AND database IN ('${CLICKHOUSE_DATABASE}', '${DB2}')"
${CLICKHOUSE_CLIENT} --query "SELECT database, table FROM system.row_policies WHERE short_name = 'xpb' AND database IN ('${CLICKHOUSE_DATABASE}', '${DB2}')"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY xpa ON ${DB2}.xb"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY xpb ON ${CLICKHOUSE_DATABASE}.xa"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.xa"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${DB2}.xb"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB2}"

# The destination side of an EXCHANGE needs its own database-wide rejection: the table arriving from
# the other database would fall under the destination `ON db.*` policy it never had, and the table
# leaving cannot take that policy with it. Only a DESTINATION-side `ON db.*` (with none on the source)
# exercises that branch; the source-side rejection tested below cannot reach it.
echo '-- cross-database EXCHANGE rejected when only the DESTINATION has a database-wide policy'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB2}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB2}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.ya (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB2}.yb (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.ya VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB2}.yb VALUES (10, 'eng'), (20, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY yp ON ${DB2}.* FOR SELECT USING dept = 'fin' TO ${USER}"
run_user "EXCHANGE TABLES ${CLICKHOUSE_DATABASE}.ya AND ${DB2}.yb" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
echo 'after rejected exchange nothing moved: ya still holds its own {1,2} unfiltered (no policy in that database) and yb still holds {10,20} filtered by the db-wide fin policy -> 20:'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.ya ORDER BY id"
run_user "SELECT id FROM ${DB2}.yb ORDER BY id"
echo 'and the db-wide policy is still bound to its own database:'
${CLICKHOUSE_CLIENT} --query "SELECT database FROM system.row_policies WHERE short_name = 'yp' AND database IN ('${CLICKHOUSE_DATABASE}', '${DB2}')"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY yp ON ${DB2}.*"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.ya"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${DB2}.yb"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB2}"

echo '-- RENAME DATABASE: database-wide and per-table policies follow'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB2}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_src"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_src.t (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}_src.t VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_src.t2 (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}_src.t2 VALUES (3, 'eng'), (4, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY dbp ON ${CLICKHOUSE_DATABASE}_src.* FOR SELECT USING dept = 'eng' TO ${USER}"
# A per-table policy in the renamed database must move to the new database keeping its table name.
# Without this case the database-wide policy alone would keep the section green even if the per-table
# branch of collectRowPolicyRekeysForDatabase were removed. Permissive policies are combined with OR,
# so this one makes the 'fin' row id=4 visible in t2 -- which happens only if it followed the rename.
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY tbp ON ${CLICKHOUSE_DATABASE}_src.t2 FOR SELECT USING id = 4 TO ${USER}"
run_user "RENAME DATABASE ${CLICKHOUSE_DATABASE}_src TO ${DB2}"
echo 'after database rename (db-wide policy still applies -> eng only):'
run_user "SELECT id FROM ${DB2}.t ORDER BY id"
echo 'and the per-table policy followed too, so t2 shows eng (3) plus the id=4 row it permits:'
run_user "SELECT id FROM ${DB2}.t2 ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT database FROM system.row_policies WHERE short_name = 'dbp' AND database = '${DB2}'"
${CLICKHOUSE_CLIENT} --query "SELECT database, table FROM system.row_policies WHERE short_name = 'tbp' AND database IN ('${CLICKHOUSE_DATABASE}_src', '${DB2}')"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY dbp ON ${DB2}.*"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY tbp ON ${DB2}.t2"
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

# `EXCHANGE TABLES t AND t` is a documented no-op that succeeds (01109_exchange_tables pins it), so it
# must keep succeeding even when a read-only policy names t: the element moves no binding, so there is
# nothing to re-key and evaluating the transition could only invent a failure. Reuses the read-only
# users.xml harness above because a writable policy cannot reach the read-only rejection at all.
echo '-- self-EXCHANGE of a table with a read-only (users.xml) policy still succeeds'
${CLICKHOUSE_LOCAL} --config-file "${LOCAL_DIR}/config.xml" --path "${LOCAL_DIR}" -q "
EXCHANGE TABLES rodb.rt AND rodb.rt;
SELECT 'self-exchange accepted';
SELECT table FROM system.row_policies WHERE database = 'rodb';
SELECT id FROM rodb.rt ORDER BY id;
" 2>&1
rm -rf "${LOCAL_DIR}"

echo '-- self-EXCHANGE with a writable policy: succeeds, binding and filtering unchanged'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.selfx (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.selfx VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY selfxp ON ${CLICKHOUSE_DATABASE}.selfx FOR SELECT USING dept = 'eng' TO ${USER}"
run_user "EXCHANGE TABLES ${CLICKHOUSE_DATABASE}.selfx AND ${CLICKHOUSE_DATABASE}.selfx"
echo 'after self-exchange (policy still on selfx, still eng only):'
${CLICKHOUSE_CLIENT} --query "SELECT table FROM system.row_policies WHERE short_name = 'selfxp' AND database = '${CLICKHOUSE_DATABASE}'"
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.selfx ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY selfxp ON ${CLICKHOUSE_DATABASE}.selfx"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.selfx"

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

# Same collision, but the occupant of the parking name is itself one of the moving policies (both
# tables of an EXCHANGE carry a policy with the same short name). A moving occupant is legitimate for
# a policy's final DESTINATION -- an EXCHANGE swaps two such policies -- but not for a transient
# parking name: phase 1 would then hit the still-occupied name and throw after the rename committed.
echo '-- EXCHANGE rejected when a moving policy occupies another policy transient name'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.ma (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.ma VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY mp ON ${CLICKHOUSE_DATABASE}.ma FOR SELECT USING dept = 'eng' TO ${USER}"
MP_ID=$(${CLICKHOUSE_CLIENT} --query "SELECT id FROM system.row_policies WHERE short_name = 'mp' AND database = '${CLICKHOUSE_DATABASE}' AND table = 'ma'")
MP_TMP=".tmp_rename_row_policy_${MP_ID}_0"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.\`${MP_TMP}\` (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.\`${MP_TMP}\` VALUES (10, 'eng'), (20, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY mp ON ${CLICKHOUSE_DATABASE}.\`${MP_TMP}\` FOR SELECT USING dept = 'fin' TO ${USER}"
run_user "EXCHANGE TABLES ${CLICKHOUSE_DATABASE}.ma AND ${CLICKHOUSE_DATABASE}.\`${MP_TMP}\`" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
echo 'after rejected exchange (nothing moved: ma still holds {1,2}, and both policies are still bound):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.ma ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.row_policies WHERE short_name = 'mp' AND database = '${CLICKHOUSE_DATABASE}' AND table IN ('ma', '${MP_TMP}')"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY mp ON ${CLICKHOUSE_DATABASE}.ma, mp ON ${CLICKHOUSE_DATABASE}.\`${MP_TMP}\`"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.ma"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.\`${MP_TMP}\`"

# CREATE OR REPLACE TABLE / REPLACE TABLE swap a freshly built table (under a temporary name) into
# the target name through a synthetic rename/exchange. Only the storage is replaced; the target keeps
# its name, so its row policy must stay bound to that name and keep filtering the new data. The policy
# must NOT follow the data onto the dropped temporary name (which would leave the table unfiltered).
echo '-- CREATE OR REPLACE TABLE: target row policy stays and keeps filtering'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.cor (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.cor VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY corp ON ${CLICKHOUSE_DATABASE}.cor FOR SELECT USING dept = 'eng' TO ${USER}"
echo 'before (eng only):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.cor ORDER BY id"
run_user "CREATE OR REPLACE TABLE ${CLICKHOUSE_DATABASE}.cor (id UInt64, dept String) ENGINE = MergeTree ORDER BY id AS SELECT 1, 'eng' UNION ALL SELECT 2, 'fin' UNION ALL SELECT 4, 'fin'"
echo 'after CREATE OR REPLACE (policy still filters -> eng only, not the new fin rows):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.cor ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT table FROM system.row_policies WHERE short_name = 'corp' AND database = '${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY corp ON ${CLICKHOUSE_DATABASE}.cor"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.cor"

echo '-- REPLACE TABLE: target row policy stays and keeps filtering'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.rep (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.rep VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY repp ON ${CLICKHOUSE_DATABASE}.rep FOR SELECT USING dept = 'eng' TO ${USER}"
run_user "REPLACE TABLE ${CLICKHOUSE_DATABASE}.rep (id UInt64, dept String) ENGINE = MergeTree ORDER BY id AS SELECT 1, 'eng' UNION ALL SELECT 2, 'fin' UNION ALL SELECT 4, 'fin'"
echo 'after REPLACE TABLE (policy still filters -> eng only):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.rep ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT table FROM system.row_policies WHERE short_name = 'repp' AND database = '${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY repp ON ${CLICKHOUSE_DATABASE}.rep"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.rep"

# A non-append refreshable materialized view installs each fresh result by exchanging a temporary
# table into the target name -- the same storage-replacing swap as CREATE OR REPLACE, so the target's
# policy must stay on the target name. Assert after the FIRST refresh: the swap is symmetric, so on
# an even number of refreshes a policy that followed the data lands back on the target name and the
# escape is invisible. Every odd refresh is when it is exposed.
echo '-- refreshable materialized view: target row policy stays and keeps filtering'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.rmvt (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY rmvp ON ${CLICKHOUSE_DATABASE}.rmvt FOR SELECT USING dept = 'eng' TO ${USER}"
# REFRESH EVERY 1 YEAR: creation triggers exactly one refresh, and no second one can race the checks.
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.rmv REFRESH EVERY 1 YEAR TO ${CLICKHOUSE_DATABASE}.rmvt (id UInt64, dept String) AS SELECT 1 AS id, 'eng' AS dept UNION ALL SELECT 2, 'fin'"
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT VIEW ${CLICKHOUSE_DATABASE}.rmv"
echo 'refreshed target really has both rows:'
# Read the count as metadata: `rmvp` targets ${USER}, so an admin-side data read of `rmvt` matches no
# policy and errors under throw_on_unmatched_row_policies, which the CI config enables.
${CLICKHOUSE_CLIENT} --query "SELECT total_rows FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'rmvt'"
echo 'after the first refresh (policy still filters -> eng only, not the fin row):'
run_user "SELECT id FROM ${CLICKHOUSE_DATABASE}.rmvt ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT table FROM system.row_policies WHERE short_name = 'rmvp' AND database = '${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.rmv"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY rmvp ON ${CLICKHOUSE_DATABASE}.rmvt"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.rmvt"

# A row policy can name a dictionary: ParserRowPolicyName parses `ON db.name` without checking the
# storage kind, and the filter is applied by resolved StorageID regardless of engine. RENAME DICTIONARY
# produces an ordinary ASTRenameQuery, so it takes the same path as a table rename -- deliberately, so
# the policy follows instead of being stranded on the old dictionary name (the same escape class).
echo '-- RENAME DICTIONARY: policy follows the dictionary'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.dsrc (id UInt64, dept String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.dsrc VALUES (1, 'eng'), (2, 'fin')"
${CLICKHOUSE_CLIENT} --query "CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict (id UInt64, dept String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'dsrc' DB '${CLICKHOUSE_DATABASE}')) LAYOUT(FLAT()) LIFETIME(0)"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY dp ON ${CLICKHOUSE_DATABASE}.dict FOR SELECT USING dept = 'eng' TO ${USER}"
echo 'before (eng only -> 1 row of 2):'
run_user "SELECT count() FROM ${CLICKHOUSE_DATABASE}.dict"
${CLICKHOUSE_CLIENT} --query "RENAME DICTIONARY ${CLICKHOUSE_DATABASE}.dict TO ${CLICKHOUSE_DATABASE}.dict2"
echo 'after rename (policy followed -> still 1 row, and bound to dict2):'
run_user "SELECT count() FROM ${CLICKHOUSE_DATABASE}.dict2"
${CLICKHOUSE_CLIENT} --query "SELECT table FROM system.row_policies WHERE short_name = 'dp' AND database = '${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY dp ON ${CLICKHOUSE_DATABASE}.dict2"

# `EXCHANGE DICTIONARIES` sets both `exchange` and `dictionary` on the `ASTRenameQuery`
# (`ParserRenameQuery`), so it reaches the same path with `exchange_tables` true and both directions
# must follow. Giving the two policies the SAME short name is the point: the swap is only
# collision-free because `applyRowPolicyRekeys` parks each policy under a unique
# `tempRekeyTableName` first, so a same-short-name exchange exercises that two-phase move.
echo '-- EXCHANGE DICTIONARIES with the same policy short name on both dictionaries'
${CLICKHOUSE_CLIENT} --query "CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.xdictA (id UInt64, dept String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'dsrc' DB '${CLICKHOUSE_DATABASE}')) LAYOUT(FLAT()) LIFETIME(0)"
${CLICKHOUSE_CLIENT} --query "CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.xdictB (id UInt64, dept String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'dsrc' DB '${CLICKHOUSE_DATABASE}')) LAYOUT(FLAT()) LIFETIME(0)"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY xdp ON ${CLICKHOUSE_DATABASE}.xdictA FOR SELECT USING dept = 'eng' TO ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY xdp ON ${CLICKHOUSE_DATABASE}.xdictB FOR SELECT USING dept = 'fin' TO ${USER}"
echo 'before (xdictA filtered to eng -> id 1, xdictB filtered to fin -> id 2):'
run_user "SELECT groupArray(id) FROM ${CLICKHOUSE_DATABASE}.xdictA"
run_user "SELECT groupArray(id) FROM ${CLICKHOUSE_DATABASE}.xdictB"
${CLICKHOUSE_CLIENT} --query "EXCHANGE DICTIONARIES ${CLICKHOUSE_DATABASE}.xdictA AND ${CLICKHOUSE_DATABASE}.xdictB"
echo 'after exchange (both policies followed their data -> the two answers swapped):'
run_user "SELECT groupArray(id) FROM ${CLICKHOUSE_DATABASE}.xdictA"
run_user "SELECT groupArray(id) FROM ${CLICKHOUSE_DATABASE}.xdictB"
${CLICKHOUSE_CLIENT} --query "SELECT table, select_filter FROM system.row_policies WHERE short_name = 'xdp' AND database = '${CLICKHOUSE_DATABASE}' ORDER BY table"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY xdp ON ${CLICKHOUSE_DATABASE}.xdictA"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY xdp ON ${CLICKHOUSE_DATABASE}.xdictB"
${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.xdictA"
${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.xdictB"

# The cross-database rejection applies to dictionaries for the same reason as to tables: an ON db.*
# policy cannot follow the object out of its database.
echo '-- cross-database RENAME DICTIONARY rejected when a database-wide (db.*) policy applies'
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY dwide ON ${CLICKHOUSE_DATABASE}.* FOR SELECT USING dept = 'eng' TO ${USER}"
${CLICKHOUSE_CLIENT} --query "RENAME DICTIONARY ${CLICKHOUSE_DATABASE}.dict2 TO ${DB2}.dict3" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
echo 'after rejected cross-db dictionary rename (dictionary not moved, db-wide policy still bound):'
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.dictionaries WHERE database = '${CLICKHOUSE_DATABASE}' ORDER BY name"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.row_policies WHERE short_name = 'dwide' AND database = '${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY dwide ON ${CLICKHOUSE_DATABASE}.*"
${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.dict2"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.dsrc"

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER}"
