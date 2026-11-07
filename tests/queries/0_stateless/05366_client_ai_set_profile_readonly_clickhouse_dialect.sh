#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A settings profile that makes the session read-only and leaves the dialect alone. It is applied in
# the middle of the session with `SET profile`, which the server does without reporting the settings
# it changed back to the client.
profile="profile_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${profile}"
# Parallel replicas are pinned off: a configuration whose default profile turns them on would
# otherwise leave a `readonly = 1` session that cannot turn them off for the internal queries of the
# agent, which then refuse to run (see `ClientBase::fetchInternalQueryResult`).
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${profile} SETTINGS readonly = 1, enable_parallel_replicas = 0"

CLICKHOUSE_AI_TEST_PROFILE="${profile}" python3 "$CUR_DIR"/05366_client_ai_set_profile_readonly_clickhouse_dialect.python

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${profile}"
