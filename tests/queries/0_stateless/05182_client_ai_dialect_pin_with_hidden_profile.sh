#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user whose settings profile selects another SQL dialect, connecting with
# `apply_settings_from_server = 0` so that the value never reaches the client: the client keeps its
# own default and only the server knows what it parses with.
user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${profile}"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${profile} SETTINGS dialect = 'kusto', allow_experimental_kusto_dialect = 1"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO ${user}"
$CLICKHOUSE_CLIENT -q "GRANT SHOW ON *.* TO ${user}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.plain"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.plain (x UInt32) ENGINE = MergeTree ORDER BY x AS SELECT 1"

# The positive control of the whole test: the profile really is in effect and really is invisible to
# the client, so ClickHouse SQL sent by this user without a `dialect` of its own reaches the KQL
# parser. Without it every expectation below could pass vacuously.
$CLICKHOUSE_CLIENT --user "${user}" --apply_settings_from_server 0 -q "SELECT 1" 2>&1 | grep -c -m1 'SYNTAX_ERROR'

CLICKHOUSE_AI_TEST_USER="${user}" python3 "$CUR_DIR"/05182_client_ai_dialect_pin_with_hidden_profile.python

$CLICKHOUSE_CLIENT -q "DROP TABLE ${CLICKHOUSE_DATABASE}.plain"
$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${profile}"
