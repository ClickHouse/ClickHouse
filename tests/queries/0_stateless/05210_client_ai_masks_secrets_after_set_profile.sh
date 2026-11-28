#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A settings profile that turns the display of the secrets of a table definition on. It is applied
# in the middle of the session with `SET profile`, which the server does without reporting the
# settings it changed back to the client - so the client keeps believing the session does not
# display them.
profile="profile_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${profile}"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${profile} SETTINGS format_display_secrets_in_show_and_select = 1"

# The positive control of the whole test: the profile really does turn the setting on for the rest
# of the session. Without it the expectation below could be met by a session where nothing changed.
$CLICKHOUSE_CLIENT -q "SET profile = '${profile}'; SELECT toUInt8(getSetting('format_display_secrets_in_show_and_select'))"

CLICKHOUSE_AI_TEST_PROFILE="${profile}" python3 "$CUR_DIR"/05210_client_ai_masks_secrets_after_set_profile.python

# The same session as a user whose own settings name the setting. The server reports such a value
# to the client in the handshake, and the client does not send a setting back to the server when
# its value is the reported one. The mask of the agent is that very value here, so it used to be
# left out of the query, and the value of the profile applied to it.
user="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} IDENTIFIED WITH no_password SETTINGS format_display_secrets_in_show_and_select = 0"
$CLICKHOUSE_CLIENT -q "GRANT ALL ON *.* TO ${user}"

# The positive control again: the profile turns the setting on for this user as well.
$CLICKHOUSE_CLIENT --user "${user}" -q "SET profile = '${profile}'; SELECT toUInt8(getSetting('format_display_secrets_in_show_and_select'))"

CLICKHOUSE_AI_TEST_PROFILE="${profile}" CLICKHOUSE_AI_TEST_USER="${user}" python3 "$CUR_DIR"/05210_client_ai_masks_secrets_after_set_profile.python

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${profile}"
