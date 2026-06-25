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

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER}"
