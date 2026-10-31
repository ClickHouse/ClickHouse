#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=${CLICKHOUSE_TMP}/05023_${CLICKHOUSE_DATABASE}
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"

local_query()
{
    $CLICKHOUSE_LOCAL --path "${WORK_DIR}" --allow_deprecated_database_ordinary 1 --query "$1"
}

local_query "CREATE DATABASE db ENGINE = Ordinary"
local_query "CREATE TABLE db.legacy (x UInt8, arr Array(UInt8), y UInt8, m Array(UInt8) MATERIALIZED arrayMap(x -> y, arr)) ENGINE = MergeTree ORDER BY tuple()"
local_query "INSERT INTO db.legacy (x, arr, y) VALUES (1, [1], 2)"

# Simulate metadata written before alias-lambda capture validation existed.
sed -i.bak 's/`y` UInt8/`y` UInt8 ALIAS `x` + 1/' "${WORK_DIR}/metadata/db/legacy.sql"

# An unrelated `CLEAR COLUMN` remains compatible even when it recomputes a different, safe
# `MATERIALIZED` column and therefore enters the table-wide rematerialization path.
local_query "ALTER TABLE db.legacy RENAME COLUMN m TO m2; ALTER TABLE db.legacy RENAME COLUMN y TO y2; ALTER TABLE db.legacy RENAME COLUMN arr TO arr2; ALTER TABLE db.legacy ADD COLUMN z UInt8, ADD COLUMN mz UInt8 MATERIALIZED z + 1; ALTER TABLE db.legacy CLEAR COLUMN z SETTINGS mutations_sync = 2; ALTER TABLE db.legacy DROP COLUMN mz; ALTER TABLE db.legacy DROP COLUMN z; SELECT 'unrelated clear succeeded'"

# Clearing a direct input, or an input reached through the `ALIAS` body, would require evaluating
# the captured `MATERIALIZED` expression. Reject both instead of hardlinking its stale value.
local_query "ALTER TABLE db.legacy CLEAR COLUMN arr2 SETTINGS mutations_sync = 2" 2>&1 \
    | grep -o 'ALTER_OF_COLUMN_IS_FORBIDDEN' | head -1
local_query "ALTER TABLE db.legacy CLEAR COLUMN x SETTINGS mutations_sync = 2" 2>&1 \
    | grep -o 'ALTER_OF_COLUMN_IS_FORBIDDEN' | head -1

local_query "SELECT m2 FROM db.legacy"

rm -rf "${WORK_DIR}"
