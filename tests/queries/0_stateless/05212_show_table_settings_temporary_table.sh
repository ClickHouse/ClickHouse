#!/usr/bin/env bash
# `SHOW TABLE SETTINGS FROM name` resolves a name without a database the way `SHOW CREATE TABLE` does: one of the
# session's temporary tables first, then a table of the current database. A temporary table lives in one
# session, so each case runs in a single client invocation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS shadowed"

echo "-- a temporary table is found by its name"
$CLICKHOUSE_CLIENT -q "
CREATE TEMPORARY TABLE tmp_only (a UInt64) ENGINE = Memory SETTINGS min_rows_to_keep = 7, max_rows_to_keep = 70;
SHOW TABLE SETTINGS FROM tmp_only LIKE '%rows_to_keep';"

echo "-- a temporary table shadows a table of the current database with the same name"
$CLICKHOUSE_CLIENT -q "CREATE TABLE shadowed (a UInt64) ENGINE = Memory SETTINGS min_rows_to_keep = 1, max_rows_to_keep = 10"
$CLICKHOUSE_CLIENT -q "
CREATE TEMPORARY TABLE shadowed (a UInt64) ENGINE = Memory SETTINGS min_rows_to_keep = 2, max_rows_to_keep = 20;
SHOW TABLE SETTINGS FROM shadowed LIKE '%rows_to_keep';
SELECT '-- naming the database still reaches the permanent table';
SHOW TABLE SETTINGS FROM ${CLICKHOUSE_DATABASE}.shadowed LIKE '%rows_to_keep';"

$CLICKHOUSE_CLIENT -q "DROP TABLE shadowed"
