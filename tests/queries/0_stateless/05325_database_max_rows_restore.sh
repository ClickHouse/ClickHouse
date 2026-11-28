#!/usr/bin/env bash
# Tags: zookeeper
# `RESTORE` of table data adds rows to the database, so it is checked against the database `max_rows`
# limit like `ATTACH PARTITION`: restoring 50 rows into a database at 90/100 rows is rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_rows"
BACKUP="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

CH="${CLICKHOUSE_CLIENT}"

$CH -q "DROP DATABASE IF EXISTS ${DB}"

$CH -q "
CREATE TABLE ${CLICKHOUSE_DATABASE}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.src SELECT number FROM numbers(50);
CREATE TABLE ${CLICKHOUSE_DATABASE}.src_replicated (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_replicated', 'r1') ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.src_replicated SELECT number FROM numbers(50);
"
$CH -q "BACKUP TABLE ${CLICKHOUSE_DATABASE}.src, TABLE ${CLICKHOUSE_DATABASE}.src_replicated TO ${BACKUP}" --format Null
# The restored replicated table reuses the ZooKeeper path of the source, so free it first.
$CH -q "DROP TABLE ${CLICKHOUSE_DATABASE}.src_replicated SYNC"

$CH -q "
CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS max_rows = 100;
CREATE TABLE ${DB}.filler (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DB}.filler SELECT number FROM numbers(90);
"

$CH -q "SELECT '-- MergeTree: RESTORE of 50 rows into a database at 90/100 rows is rejected'"
$CH -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.src AS ${DB}.restored FROM ${BACKUP}" --format Null 2>&1 | grep -o -m1 'TOO_MANY_ROWS'
$CH -q "SELECT rows FROM system.databases WHERE name = '${DB}'"

$CH -q "SELECT '-- ReplicatedMergeTree: the same RESTORE is rejected'"
$CH -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.src_replicated AS ${DB}.restored_replicated FROM ${BACKUP}" --format Null 2>&1 | grep -o -m1 'TOO_MANY_ROWS'
$CH -q "SELECT rows FROM system.databases WHERE name = '${DB}'"

$CH -q "SELECT '-- with enough headroom, RESTORE succeeds'"
$CH -q "DROP TABLE IF EXISTS ${DB}.restored SYNC; DROP TABLE IF EXISTS ${DB}.restored_replicated SYNC; TRUNCATE TABLE ${DB}.filler"
$CH -q "INSERT INTO ${DB}.filler SELECT number FROM numbers(30)"
$CH -q "RESTORE TABLE ${CLICKHOUSE_DATABASE}.src AS ${DB}.restored FROM ${BACKUP}" --format Null
$CH -q "SELECT rows FROM system.databases WHERE name = '${DB}'"

$CH -q "DROP DATABASE ${DB} SYNC"
