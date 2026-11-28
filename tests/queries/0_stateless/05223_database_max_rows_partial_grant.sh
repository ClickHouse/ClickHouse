#!/usr/bin/env bash
# `system.databases.rows` sums every counted table in the database. A user who can see the database only
# through a partial grant (e.g. SELECT on a single table) must not be able to read that total, otherwise the
# sizes of the tables hidden from them could be inferred by subtraction.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_rows"
USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

CH="${CLICKHOUSE_CLIENT}"

$CH -q "DROP DATABASE IF EXISTS ${DB}; DROP USER IF EXISTS ${USER}"

$CH -q "
CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 100;
CREATE TABLE ${DB}.visible (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${DB}.hidden (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DB}.visible SELECT number FROM numbers(3);
INSERT INTO ${DB}.hidden SELECT number FROM numbers(40);
CREATE USER ${USER};
GRANT SELECT(x) ON ${DB}.visible TO ${USER};
"

$CH -q "SELECT '-- the owner sees the exact database total'; SELECT rows FROM system.databases WHERE name = '${DB}'"

$CH -q "SELECT '-- a partial grant makes the database visible, but not its row total'"
$CH --user "${USER}" -q "SELECT name = '${DB}', rows FROM system.databases WHERE name = '${DB}'"
$CH --user "${USER}" -q "SELECT name, total_rows FROM system.tables WHERE database = '${DB}' ORDER BY name"

$CH -q "SELECT '-- SHOW TABLES on a single table is still a partial grant'"
$CH -q "GRANT SHOW TABLES ON ${DB}.hidden TO ${USER}"
$CH --user "${USER}" -q "SELECT rows FROM system.databases WHERE name = '${DB}'"

$CH -q "SELECT '-- SHOW TABLES on the whole database reveals the total'"
$CH -q "GRANT SHOW TABLES ON ${DB}.* TO ${USER}"
$CH --user "${USER}" -q "SELECT rows FROM system.databases WHERE name = '${DB}'"

$CH -q "SELECT '-- ... and so does the global grant'"
$CH -q "REVOKE SHOW TABLES ON ${DB}.* FROM ${USER}; GRANT SHOW TABLES ON *.* TO ${USER}"
$CH --user "${USER}" -q "SELECT rows FROM system.databases WHERE name = '${DB}'"

$CH -q "DROP USER ${USER}; DROP DATABASE ${DB}"
