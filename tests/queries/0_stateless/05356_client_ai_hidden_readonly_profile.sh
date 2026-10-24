#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user whose settings profile makes the session read-only, connecting with
# `apply_settings_from_server = 0` so that the value never reaches the client.
user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${profile}"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${profile} SETTINGS readonly = 1"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO ${user}"

CLICKHOUSE_AI_TEST_USER="${user}" python3 "$CUR_DIR"/05356_client_ai_hidden_readonly_profile.python

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${profile}"
