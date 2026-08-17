#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The alias-lambda-capture rule is newer than some stored metadata, and metadata loading skips it,
# so a table whose stored default already violates it still loads. An unrelated ALTER on such a
# table must not start failing (the rule would retroactively reject unchanged legacy metadata),
# while an ALTER that introduces a new violation is still rejected.

WORK_DIR=${CLICKHOUSE_TMP}/04873_${CLICKHOUSE_DATABASE}
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"

local_query()
{
    $CLICKHOUSE_LOCAL --path "${WORK_DIR}" --allow_deprecated_database_ordinary 1 --query "$1"
}

# `y` is an ordinary column here, so the definition is valid and creatable.
local_query "CREATE DATABASE db ENGINE = Ordinary"
local_query "CREATE TABLE db.legacy (x UInt8, arr Array(UInt8), y UInt8, m Array(UInt8) MATERIALIZED arrayMap(x -> y, arr)) ENGINE = MergeTree ORDER BY tuple()"
local_query "INSERT INTO db.legacy (x, arr, y) VALUES (1, [1], 2)"

# Simulate legacy metadata predating the capture rule: turn `y` into `ALIAS x + 1`, which makes
# the expansion of `y` inside `arrayMap(x -> y, arr)` a capture the rule would reject at CREATE.
sed -i "s/\`y\` UInt8/\`y\` UInt8 ALIAS \`x\` + 1/" "${WORK_DIR}/metadata/db/legacy.sql"

# The table still loads, and unrelated ALTERs keep working. In particular, `CLEAR COLUMN` must
# not revalidate the unrelated legacy MATERIALIZED expression while determining which columns to
# recompute for its mutation.
local_query "ALTER TABLE db.legacy RENAME COLUMN m TO m2; ALTER TABLE db.legacy RENAME COLUMN y TO y2; ALTER TABLE db.legacy ADD COLUMN z UInt8; ALTER TABLE db.legacy CLEAR COLUMN z SETTINGS mutations_sync = 2; ALTER TABLE db.legacy DROP COLUMN z; ALTER TABLE db.legacy RENAME COLUMN arr TO arr2; SELECT 'unrelated ALTERs succeeded'"

# An ALTER that introduces a new violation is still rejected.
local_query "ALTER TABLE db.legacy ADD COLUMN m3 Array(UInt8) MATERIALIZED arrayMap(x -> y2, arr2)" 2>&1 \
    | grep -o "BAD_ARGUMENTS" | head -1

# A dropped legacy violation must not be transferred to a different MATERIALIZED column renamed
# into the dropped name. The matcher in `m` changes when `b` is added, so the renamed column must
# be rematerialized instead of keeping the old value from existing parts.
local_query "CREATE TABLE db.drop_rename (a UInt64, x UInt8, arr Array(UInt8), y UInt8, c Array(UInt8) MATERIALIZED arrayMap(x -> y, arr), m UInt64 MATERIALIZED greatest(COLUMNS('^(a|b)$'))) ENGINE = MergeTree ORDER BY a"
local_query "INSERT INTO db.drop_rename (a, x, arr, y) VALUES (1, 1, [1], 2)"
sed -i 's/`y` UInt8/`y` UInt8 ALIAS `x` + 1/' "${WORK_DIR}/metadata/db/drop_rename.sql"
local_query "ALTER TABLE db.drop_rename DROP COLUMN c, RENAME COLUMN m TO c, ADD COLUMN b UInt64 DEFAULT a + 1000 SETTINGS mutations_sync = 2"
local_query "SELECT a, b, c FROM db.drop_rename"

rm -rf "${WORK_DIR}"
