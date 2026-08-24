#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: a background mutation selects its analyzer from the background context, so a
# session `enable_analyzer` cannot reach the `ALTER ... UPDATE` arm.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A settings profile is server-global, so the name carries the database to stay parallel-safe.
PROFILE_OFF="p_off_${CLICKHOUSE_DATABASE}"
PROFILE_ON="p_on_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${PROFILE_OFF}, ${PROFILE_ON}"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${PROFILE_OFF} SETTINGS enable_global_with_statement = 0"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${PROFILE_ON}  SETTINGS enable_global_with_statement = 1"

# `src` exists in the session database and in the updated table's database, with different contents.
$CLICKHOUSE_CLIENT -q "CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (99)"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_1"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_1.src (id UInt64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${CLICKHOUSE_DATABASE}_1.src VALUES (2)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_1.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${CLICKHOUSE_DATABASE}_1.t VALUES (1, 0)"

# A profile names a group of settings, so it reaches `enable_global_with_statement` without
# naming it. With the setting off the nested `SELECT` does not see the enclosing alias, so `src`
# there is the updated table's own `src` (2), the same value the query returns when run directly.
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${CLICKHOUSE_DATABASE}_1.t
    UPDATE v = (WITH src AS (SELECT 7 AS id)
                SELECT (SELECT max(id) FROM src SETTINGS profile = '${PROFILE_OFF}'))
    WHERE id = 1 SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT -q "SELECT 'P1', v FROM ${CLICKHOUSE_DATABASE}_1.t WHERE id = 1"

# The same clause with a profile that leaves the setting on: the name is the alias (7).
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${CLICKHOUSE_DATABASE}_1.t
    UPDATE v = (WITH src AS (SELECT 7 AS id)
                SELECT (SELECT max(id) FROM src SETTINGS profile = '${PROFILE_ON}'))
    WHERE id = 1 SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT -q "SELECT 'P2', v FROM ${CLICKHOUSE_DATABASE}_1.t WHERE id = 1"

# A profile written after an explicit value overrides it, so the name is the alias (7).
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${CLICKHOUSE_DATABASE}_1.t
    UPDATE v = (WITH src AS (SELECT 7 AS id)
                SELECT (SELECT max(id) FROM src
                        SETTINGS enable_global_with_statement = 0, profile = '${PROFILE_ON}'))
    WHERE id = 1 SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT -q "SELECT 'P3', v FROM ${CLICKHOUSE_DATABASE}_1.t WHERE id = 1"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_1"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${PROFILE_OFF}, ${PROFILE_ON}"
