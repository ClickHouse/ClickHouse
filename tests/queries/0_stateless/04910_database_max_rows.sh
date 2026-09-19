#!/usr/bin/env bash
# Tests the per-database `max_rows` setting: INSERT/ATTACH/rename/exchange enforcement,
# ALTER DATABASE MODIFY SETTING, and the system.databases.rows column.
# Statements are batched into few client invocations to keep the test fast under
# sanitizers; statements that must fail run in separate invocations.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DA="${CLICKHOUSE_DATABASE}_a"
DB="${CLICKHOUSE_DATABASE}_b"

CH="${CLICKHOUSE_CLIENT}"

$CH -q "DROP DATABASE IF EXISTS ${DA}; DROP DATABASE IF EXISTS ${DB}"

$CH -q "
SELECT '-- 1. setting is stored and visible in system.databases';
CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 10;
SELECT engine_full LIKE '%max_rows = 10%' FROM system.databases WHERE name = '${DA}';
SELECT '-- 2. counter tracks inserts and matches system.tables';
CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DA}.t SELECT number FROM numbers(8);
SELECT rows FROM system.databases WHERE name = '${DA}';
SELECT rows = (SELECT sum(total_rows) FROM system.tables WHERE database = '${DA}') FROM system.databases WHERE name = '${DA}';
SELECT '-- 3. a single batch may overshoot the limit, the next insert throws';
INSERT INTO ${DA}.t SELECT number FROM numbers(5);
SELECT rows FROM system.databases WHERE name = '${DA}';
"
# current 13 >= 10, the next insert is rejected
$CH -q "INSERT INTO ${DA}.t SELECT 1" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1

$CH -q "
SELECT '-- 4. TRUNCATE frees rows';
TRUNCATE TABLE ${DA}.t;
SELECT rows FROM system.databases WHERE name = '${DA}';
INSERT INTO ${DA}.t SELECT number FROM numbers(3);
SELECT rows FROM system.databases WHERE name = '${DA}';
SELECT '-- 4a. MOVE PARTITION inside an over-limit database succeeds';
DROP TABLE ${DA}.t;
CREATE TABLE ${DA}.src (d Date, x UInt64) ENGINE = MergeTree PARTITION BY d ORDER BY x;
CREATE TABLE ${DA}.dst (d Date, x UInt64) ENGINE = MergeTree PARTITION BY d ORDER BY x;
-- The first INSERT may overshoot the limit, but moving its partition to another table in the
-- same database does not change the database row count.
INSERT INTO ${DA}.src SELECT toDate('2020-01-01'), number FROM numbers(12);
ALTER TABLE ${DA}.src MOVE PARTITION '2020-01-01' TO TABLE ${DA}.dst;
SELECT count() FROM ${DA}.dst;
SELECT '-- 5. DROP PARTITION lowers the counter';
DROP TABLE ${DA}.src;
DROP TABLE ${DA}.dst;
CREATE TABLE ${DA}.p (d Date, x UInt64) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO ${DA}.p VALUES ('2020-01-01', 1), ('2020-01-01', 2), ('2020-01-02', 3);
SELECT rows FROM system.databases WHERE name = '${DA}';
ALTER TABLE ${DA}.p DROP PARTITION '2020-01-01';
SELECT rows FROM system.databases WHERE name = '${DA}';
SELECT '-- 6. ALTER DATABASE MODIFY SETTING raises and lowers the limit';
DROP TABLE ${DA}.p;
CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DA}.t SELECT number FROM numbers(9);
ALTER DATABASE ${DA} MODIFY SETTING max_rows = 100;
SELECT engine_full LIKE '%max_rows = 100%' FROM system.databases WHERE name = '${DA}';
INSERT INTO ${DA}.t SELECT number FROM numbers(50);
SELECT rows FROM system.databases WHERE name = '${DA}';
ALTER DATABASE ${DA} MODIFY SETTING max_rows = 10;
"
# the limit was lowered below the current row count: the next insert throws
$CH -q "INSERT INTO ${DA}.t SELECT 1" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1

$CH -q "
SELECT '-- 7. max_rows = 0 means unlimited';
ALTER DATABASE ${DA} MODIFY SETTING max_rows = 0;
INSERT INTO ${DA}.t SELECT number FROM numbers(1000);
SELECT rows FROM system.databases WHERE name = '${DA}';
SELECT '-- 8. only max_rows is alterable, bad values rejected';
"
$CH -q "ALTER DATABASE ${DA} MODIFY SETTING disk = 'default'" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -n1
$CH -q "ALTER DATABASE ${DA} MODIFY SETTING max_rows = -1" 2>&1 | grep -qE "Exception|Cannot" && echo "rejected"

$CH -q "
SELECT '-- 9. ATTACH of a populated table is checked';
DROP DATABASE ${DA};
CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 100;
CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DA}.t SELECT number FROM numbers(60);
DETACH TABLE ${DA}.t;
SELECT rows FROM system.databases WHERE name = '${DA}';
CREATE TABLE ${DA}.filler (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DA}.filler SELECT number FROM numbers(60);
"
# attaching t (60) on top of filler (60) exceeds 100
$CH -q "ATTACH TABLE ${DA}.t" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
# free headroom, then attach succeeds
$CH -q "
DROP TABLE ${DA}.filler;
ATTACH TABLE ${DA}.t;
SELECT rows FROM system.databases WHERE name = '${DA}';
SELECT '-- 9a. ATTACH PARTITION on a local MergeTree table is all-or-nothing';
DROP TABLE ${DA}.t;
CREATE TABLE ${DA}.m (x UInt64) ENGINE = MergeTree ORDER BY x;
-- Two separate parts in the detached directory: the first one alone would still fit into the
-- database, so a per-part check would make it visible before the second part throws.
SYSTEM STOP MERGES ${DA}.m;
INSERT INTO ${DA}.m SELECT number FROM numbers(10);
INSERT INTO ${DA}.m SELECT number + 10 FROM numbers(10);
ALTER TABLE ${DA}.m DETACH PARTITION ALL;
CREATE TABLE ${DA}.filler (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DA}.filler SELECT number FROM numbers(90);
SELECT rows FROM system.databases WHERE name = '${DA}';
"
# 20 detached rows do not fit into the remaining 10, and no part may become visible
$CH -q "ALTER TABLE ${DA}.m ATTACH PARTITION ALL" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
$CH -q "SELECT count() FROM ${DA}.m"
$CH -q "
DROP TABLE ${DA}.filler;
ALTER TABLE ${DA}.m ATTACH PARTITION ALL;
SELECT count() FROM ${DA}.m;
SELECT rows FROM system.databases WHERE name = '${DA}';
DROP TABLE ${DA}.m;
SELECT '-- 10. max_rows and lazy_load_tables cannot be combined';
"
$CH -q "CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 5, lazy_load_tables = 1" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -n1
$CH -q "CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
$CH -q "ALTER DATABASE ${DB} MODIFY SETTING max_rows = 5" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -n1

$CH --allow_deprecated_database_ordinary=1 --send_logs_level=fatal -q "
DROP DATABASE ${DB};
SELECT '-- 11. RENAME into a full Ordinary database is rejected without orphaning the table';
DROP DATABASE ${DA};
CREATE DATABASE ${DA} ENGINE = Ordinary SETTINGS max_rows = 1000;
CREATE DATABASE ${DB} ENGINE = Ordinary SETTINGS max_rows = 40;
CREATE TABLE ${DA}.big (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DA}.big SELECT number FROM numbers(50);
"
$CH -q "RENAME TABLE ${DA}.big TO ${DB}.big" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
# the source table must stay fully intact (not orphaned by a partial move)
$CH -q "
SELECT concat('da=', toString((SELECT rows FROM system.databases WHERE name = '${DA}')), ' db=', toString((SELECT rows FROM system.databases WHERE name = '${DB}')));
SELECT count() FROM ${DA}.big;
SELECT '-- 12. lazy proxy forwards rows for reporting and cross-database RENAME';
DROP DATABASE ${DA};
DROP DATABASE ${DB};
CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 40;
CREATE TABLE ${DA}.big (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DA}.big SELECT number FROM numbers(50);
DETACH DATABASE ${DA};
ATTACH DATABASE ${DA};
-- Reading database rows must materialize the proxy and report its active rows.
SELECT rows FROM system.databases WHERE name = '${DA}';
"
$CH -q "RENAME TABLE ${DA}.big TO ${DB}.big" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
$CH --allow_deprecated_database_ordinary=1 --send_logs_level=fatal -q "
EXISTS TABLE ${DA}.big;
SELECT '-- 13. RENAME inside an over-limit Ordinary database succeeds';
DROP DATABASE ${DA};
DROP DATABASE ${DB};
CREATE DATABASE ${DA} ENGINE = Ordinary SETTINGS max_rows = 10;
CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
-- A single batch can exceed the limit. Renaming it inside the database must remain a no-op for accounting.
INSERT INTO ${DA}.t SELECT number FROM numbers(12);
RENAME TABLE ${DA}.t TO ${DA}.u;
EXISTS TABLE ${DA}.u;
SELECT rows FROM system.databases WHERE name = '${DA}';
DROP DATABASE ${DA};
"
