#!/usr/bin/env bash
# `TOO_MANY_ROWS` errors must not show the database total, which `system.databases.rows` hides from a user who can
# see only some of the tables: such a user could infer the sizes of hidden tables by subtraction.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_rows"
OTHER_DB="${CLICKHOUSE_DATABASE}_other"
USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

CH="${CLICKHOUSE_CLIENT} --async_insert=0"

$CH -q "DROP DATABASE IF EXISTS ${DB}; DROP DATABASE IF EXISTS ${OTHER_DB}; DROP USER IF EXISTS ${USER}"

$CH -q "
CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 100;
CREATE DATABASE ${OTHER_DB} ENGINE = Atomic;
CREATE TABLE ${DB}.visible (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${DB}.staging (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${DB}.detached (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${DB}.hidden (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${OTHER_DB}.moved (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DB}.visible SELECT number FROM numbers(3);
INSERT INTO ${DB}.staging SELECT number FROM numbers(5);
INSERT INTO ${DB}.detached SELECT number FROM numbers(20);
INSERT INTO ${DB}.hidden SELECT number FROM numbers(90);
DETACH TABLE ${DB}.detached;
INSERT INTO ${OTHER_DB}.moved SELECT number FROM numbers(7);
CREATE USER ${USER};
GRANT SELECT, INSERT ON ${DB}.visible TO ${USER};
GRANT SELECT ON ${DB}.staging TO ${USER};
GRANT CREATE TABLE ON ${DB}.detached TO ${USER};
GRANT TABLE ENGINE ON MergeTree TO ${USER};
GRANT SELECT, DROP TABLE ON ${OTHER_DB}.moved TO ${USER};
GRANT CREATE TABLE, INSERT ON ${DB}.moved TO ${USER};
"

# The first error message (the server log repeats it), with the database name and the table UUID normalized.
message() { grep -m1 -o -E '(Adding|Moving|Too many) .*\(TOO_MANY_ROWS\)' | sed -E "s/${DB}/db/g; s/ \([0-9a-f-]{36}\)//"; }

$CH -q "SELECT '-- a partition import'"
$CH --user "${USER}" -q "ALTER TABLE ${DB}.visible ATTACH PARTITION tuple() FROM ${DB}.staging" 2>&1 | message

$CH -q "SELECT '-- a table attachment'"
$CH --user "${USER}" -q "ATTACH TABLE ${DB}.detached" 2>&1 | message

$CH -q "SELECT '-- a cross-database RENAME'"
$CH --user "${USER}" -q "RENAME TABLE ${OTHER_DB}.moved TO ${DB}.moved" 2>&1 | message

$CH -q "SELECT '-- an insert after the limit is reached'"
$CH -q "INSERT INTO ${DB}.hidden SELECT number FROM numbers(10)"
$CH --user "${USER}" -q "INSERT INTO ${DB}.visible VALUES (0)" 2>&1 | message

$CH -q "ALTER DATABASE ${DB} MODIFY SETTING max_rows = 0; ATTACH TABLE ${DB}.detached"
$CH -q "DROP USER ${USER}; DROP DATABASE ${DB}; DROP DATABASE ${OTHER_DB}"
