#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user whose settings profile selects another SQL dialect, connecting with
# `apply_settings_from_server = 0` so that the value never reaches the client, in a `readonly = 1`
# session that refuses the `dialect = 'clickhouse'` pin of the queries of the AI agent.
user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${profile}"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${profile} SETTINGS dialect = 'kusto', allow_experimental_kusto_dialect = 1"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO ${user}"

CLICKHOUSE_AI_TEST_USER="${user}" python3 "$CUR_DIR"/05323_client_ai_readonly_preflight_hidden_dialect.python

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${profile}"
