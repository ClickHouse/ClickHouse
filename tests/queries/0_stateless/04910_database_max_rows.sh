#!/usr/bin/env bash
# Tests the per-database `max_rows` setting: INSERT/ATTACH/rename/exchange enforcement,
# ALTER DATABASE MODIFY SETTING, partition imports, and the system.databases.rows column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DA="${CLICKHOUSE_DATABASE}_a"
DB="${CLICKHOUSE_DATABASE}_b"

CH="${CLICKHOUSE_CLIENT}"

cleanup() {
    $CH -q "DROP DATABASE IF EXISTS ${DA}"
    $CH -q "DROP DATABASE IF EXISTS ${DB}"
}
cleanup

# The number of rows in a database as seen by system.databases.
db_rows() { $CH -q "SELECT rows FROM system.databases WHERE name = '$1'"; }

echo "-- 1. setting is stored and visible in system.databases"
$CH -q "CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 10"
$CH -q "SELECT engine_full LIKE '%max_rows = 10%' FROM system.databases WHERE name = '${DA}'"

echo "-- 2. counter tracks inserts and matches system.tables"
$CH -q "CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(8)"
db_rows "${DA}"
$CH -q "SELECT rows = (SELECT sum(total_rows) FROM system.tables WHERE database = '${DA}') FROM system.databases WHERE name = '${DA}'"

echo "-- 3. a single batch may overshoot the limit, the next insert throws"
# current 8 < 10, so this insert of 5 is allowed and overshoots to 13
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(5)"
db_rows "${DA}"
# now 13 >= 10, the next insert is rejected
$CH -q "INSERT INTO ${DA}.t SELECT 1" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1

echo "-- 4. TRUNCATE frees rows"
$CH -q "TRUNCATE TABLE ${DA}.t"
db_rows "${DA}"
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(3)"
db_rows "${DA}"

echo "-- 4a. MOVE PARTITION inside an over-limit database succeeds"
$CH -q "DROP TABLE ${DA}.t"
$CH -q "CREATE TABLE ${DA}.src (d Date, x UInt64) ENGINE = MergeTree PARTITION BY d ORDER BY x"
$CH -q "CREATE TABLE ${DA}.dst (d Date, x UInt64) ENGINE = MergeTree PARTITION BY d ORDER BY x"
# The first INSERT may overshoot the limit, but moving its partition to another table in the
# same database does not change the database row count.
$CH -q "INSERT INTO ${DA}.src SELECT toDate('2020-01-01'), number FROM numbers(12)"
$CH -q "ALTER TABLE ${DA}.src MOVE PARTITION '2020-01-01' TO TABLE ${DA}.dst"
$CH -q "SELECT count() FROM ${DA}.dst"

echo "-- 5. DROP PARTITION lowers the counter"
$CH -q "DROP TABLE ${DA}.src"
$CH -q "DROP TABLE ${DA}.dst"
$CH -q "CREATE TABLE ${DA}.p (d Date, x UInt64) ENGINE = MergeTree PARTITION BY d ORDER BY x"
$CH -q "INSERT INTO ${DA}.p VALUES ('2020-01-01', 1), ('2020-01-01', 2), ('2020-01-02', 3)"
db_rows "${DA}"
$CH -q "ALTER TABLE ${DA}.p DROP PARTITION '2020-01-01'"
db_rows "${DA}"

echo "-- 6. ALTER DATABASE MODIFY SETTING raises and lowers the limit"
$CH -q "DROP TABLE ${DA}.p"
$CH -q "CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(9)"
$CH -q "ALTER DATABASE ${DA} MODIFY SETTING max_rows = 100"
$CH -q "SELECT engine_full LIKE '%max_rows = 100%' FROM system.databases WHERE name = '${DA}'"
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(50)"
db_rows "${DA}"
# lower below current: the next insert throws
$CH -q "ALTER DATABASE ${DA} MODIFY SETTING max_rows = 10"
$CH -q "INSERT INTO ${DA}.t SELECT 1" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1

echo "-- 7. max_rows = 0 means unlimited"
$CH -q "ALTER DATABASE ${DA} MODIFY SETTING max_rows = 0"
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(1000)"
db_rows "${DA}"

echo "-- 8. only max_rows is alterable, bad values rejected"
$CH -q "ALTER DATABASE ${DA} MODIFY SETTING disk = 'default'" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -n1
$CH -q "ALTER DATABASE ${DA} MODIFY SETTING max_rows = -1" 2>&1 | grep -qE "Exception|Cannot" && echo "rejected"

echo "-- 9. ATTACH of a populated table is checked"
cleanup
$CH -q "CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 100"
$CH -q "CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(60)"
$CH -q "DETACH TABLE ${DA}.t"
db_rows "${DA}"
$CH -q "CREATE TABLE ${DA}.filler (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.filler SELECT number FROM numbers(60)"
# attaching t (60) on top of filler (60) exceeds 100
$CH -q "ATTACH TABLE ${DA}.t" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
# free headroom, then attach succeeds
$CH -q "DROP TABLE ${DA}.filler"
$CH -q "ATTACH TABLE ${DA}.t"
db_rows "${DA}"

echo "-- 10. cross-database RENAME moves the counter"
cleanup
$CH -q "CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 1000"
$CH -q "CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 1000"
$CH -q "CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(50)"
echo "before: da=$(db_rows "${DA}") db=$(db_rows "${DB}")"
$CH -q "RENAME TABLE ${DA}.t TO ${DB}.t"
echo "after:  da=$(db_rows "${DA}") db=$(db_rows "${DB}")"

echo "-- 11. EXCHANGE across databases keeps both counters correct"
$CH -q "CREATE TABLE ${DA}.u (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.u SELECT number FROM numbers(30)"
# now DA.u=30, DB.t=50
$CH -q "EXCHANGE TABLES ${DA}.u AND ${DB}.t"
echo "after exchange: da=$(db_rows "${DA}") db=$(db_rows "${DB}")"

echo "-- 12. cross-database RENAME into a full database is rejected"
cleanup
$CH -q "CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 1000"
$CH -q "CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 40"
$CH -q "CREATE TABLE ${DA}.big (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.big SELECT number FROM numbers(50)"
# moving big (50 rows) into DB (limit 40) would exceed it
$CH -q "RENAME TABLE ${DA}.big TO ${DB}.big" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
# the table stays in its original database
echo "da=$(db_rows "${DA}") db=$(db_rows "${DB}")"

echo "-- 13. EXCHANGE that would overflow a destination is rejected"
cleanup
$CH -q "CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 1000"
$CH -q "CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 40"
$CH -q "CREATE TABLE ${DA}.huge (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.huge SELECT number FROM numbers(50)"
$CH -q "CREATE TABLE ${DB}.small (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DB}.small SELECT number FROM numbers(10)"
# DB would go 10 -> 50 (loses small, gains huge), over its limit of 40
$CH -q "EXCHANGE TABLES ${DA}.huge AND ${DB}.small" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
# both tables stay put
echo "da=$(db_rows "${DA}") db=$(db_rows "${DB}")"

echo "-- 14. RENAME DATABASE keeps the counter and the setting"
cleanup
$CH -q "CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 1000"
$CH -q "CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(40)"
$CH -q "RENAME DATABASE ${DA} TO ${DB}"
db_rows "${DB}"
$CH -q "SELECT engine_full LIKE '%max_rows = 1000%' FROM system.databases WHERE name = '${DB}'"

echo "-- 15. DETACH + ATTACH DATABASE reseeds the counter"
$CH -q "DETACH DATABASE ${DB}"
$CH -q "ATTACH DATABASE ${DB}"
db_rows "${DB}"

echo "-- 16. non-MergeTree tables do not consume max_rows headroom"
cleanup
$CH -q "CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 5"
$CH -q "CREATE TABLE ${DA}.l (x UInt64) ENGINE = Log"
$CH -q "INSERT INTO ${DA}.l SELECT number FROM numbers(100)"
db_rows "${DA}"
# a MergeTree table can still use the full budget alongside the Log table
$CH -q "CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(4)"
db_rows "${DA}"

echo "-- 17. materialized view inner table counts toward the limit"
cleanup
$CH -q "CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 1000"
$CH -q "CREATE TABLE ${DA}.src (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "CREATE MATERIALIZED VIEW ${DA}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM ${DA}.src"
$CH -q "INSERT INTO ${DA}.src SELECT number FROM numbers(20)"
# src (20) + mv inner table (20) = 40
db_rows "${DA}"

echo "-- 18. max_rows and lazy_load_tables cannot be combined"
$CH -q "DROP DATABASE IF EXISTS ${DB}"
$CH -q "CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 5, lazy_load_tables = 1" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -n1
$CH -q "CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
$CH -q "ALTER DATABASE ${DB} MODIFY SETTING max_rows = 5" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -n1
$CH -q "DROP DATABASE ${DB}"

echo "-- 19. RENAME into a full Ordinary database is rejected without orphaning the table"
cleanup
$CH --allow_deprecated_database_ordinary=1 --send_logs_level=fatal -q "CREATE DATABASE ${DA} ENGINE = Ordinary SETTINGS max_rows = 1000"
$CH --allow_deprecated_database_ordinary=1 --send_logs_level=fatal -q "CREATE DATABASE ${DB} ENGINE = Ordinary SETTINGS max_rows = 40"
$CH -q "CREATE TABLE ${DA}.big (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.big SELECT number FROM numbers(50)"
$CH -q "RENAME TABLE ${DA}.big TO ${DB}.big" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
# the source table must stay fully intact (not orphaned by a partial move)
echo "da=$(db_rows "${DA}") db=$(db_rows "${DB}")"
$CH -q "SELECT count() FROM ${DA}.big"

echo "-- 20. lazy proxy forwards rows for reporting and cross-database RENAME"
cleanup
$CH -q "CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
$CH -q "CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 40"
$CH -q "CREATE TABLE ${DA}.big (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.big SELECT number FROM numbers(50)"
$CH -q "DETACH DATABASE ${DA}; ATTACH DATABASE ${DA}"
# Reading database rows must materialize the proxy and report its active rows.
db_rows "${DA}"
$CH -q "RENAME TABLE ${DA}.big TO ${DB}.big" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1
$CH -q "EXISTS TABLE ${DA}.big"

echo "-- 21. RENAME inside an over-limit Ordinary database succeeds"
cleanup
$CH --allow_deprecated_database_ordinary=1 --send_logs_level=fatal -q "CREATE DATABASE ${DA} ENGINE = Ordinary SETTINGS max_rows = 10"
$CH -q "CREATE TABLE ${DA}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
# A single batch can exceed the limit. Renaming it inside the database must remain a no-op for accounting.
$CH -q "INSERT INTO ${DA}.t SELECT number FROM numbers(12)"
$CH -q "RENAME TABLE ${DA}.t TO ${DA}.u"
$CH -q "EXISTS TABLE ${DA}.u"
db_rows "${DA}"

echo "-- 22. ATTACH PARTITION into a full database is rejected"
cleanup
$CH -q "CREATE DATABASE ${DA} ENGINE = Atomic SETTINGS max_rows = 10"
$CH -q "CREATE TABLE ${DA}.p (d Date, x UInt64) ENGINE = MergeTree PARTITION BY d ORDER BY x"
# A first oversized INSERT is allowed, so it can provide a detached partition.
$CH -q "INSERT INTO ${DA}.p SELECT toDate('2020-01-01'), number FROM numbers(100)"
$CH -q "ALTER TABLE ${DA}.p DETACH PARTITION '2020-01-01'"
$CH -q "CREATE TABLE ${DA}.filler (x UInt64) ENGINE = MergeTree ORDER BY x"
$CH -q "INSERT INTO ${DA}.filler SELECT number FROM numbers(5)"
$CH -q "ALTER TABLE ${DA}.p ATTACH PARTITION '2020-01-01'" 2>&1 | grep -oF "TOO_MANY_ROWS" | head -n1

cleanup
