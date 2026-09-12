#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A custom setting (enabled by `custom_settings_prefixes`) can hold an AST rather than a literal, e.g.
# `custom_x = disk(type = 's3', ...)`. Assigning such a setting a second time compares the new value
# with the one the session already holds, and both sides being AST-valued is the only way to reach
# that comparison. `custom_x` lives for the lifetime of an HTTP session, so every statement below runs
# in one session.

SESSION="05153_$CLICKHOUSE_DATABASE"

# A successful SET writes nothing, so no request below is redirected: an exception body would land in
# the output and no longer match the reference.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION" \
    --data-binary "SET custom_x = disk(type = 's3', name = 'c05153first', secret_access_key = 'c05153secret')"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION" \
    --data-binary "SET custom_x = disk(type = 's3', name = 'c05153second', secret_access_key = 'c05153secret')"

# The second value wins. `name` is one of the arguments the credential masker leaves visible.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION" \
    --data-binary "SELECT
        position(value, 'c05153second') > 0,
        position(value, 'c05153first') = 0
    FROM system.settings WHERE name = 'custom_x'"

# Re-assigning the identical value is accepted too.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION" \
    --data-binary "SET custom_x = disk(type = 's3', name = 'c05153second', secret_access_key = 'c05153secret')"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION" \
    --data-binary "SELECT position(value, 'c05153second') > 0 FROM system.settings WHERE name = 'custom_x'"

# A nested SETTINGS clause is clamped against the session value rather than refused, which reaches the
# same comparison through a different caller. The inner scope sees the nested value.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION" \
    --data-binary "SELECT * FROM (
        SELECT position(value, 'c05153nested') > 0
        FROM system.settings WHERE name = 'custom_x'
        SETTINGS custom_x = disk(type = 's3', name = 'c05153nested')
    )"

# Under `readonly = 1` a setting change is refused, but an assignment the constraint check judges
# unchanged returns before that check (`SettingsConstraints.cpp:362`), so the comparison's verdict is
# observable: an exact repeat stays silent, while a value differing only in its credential is refused.
# `readonly = 1` also refuses the settings the test runner appends to the URL, so this session's URL
# carries nothing but the session id.
RO_SESSION="05153_ro_$CLICKHOUSE_DATABASE"
RO_URL="${CLICKHOUSE_URL%%\?*}?session_id=$RO_SESSION"

${CLICKHOUSE_CURL} -sS "$RO_URL" \
    --data-binary "SET custom_x = disk(type = 's3', name = 'c05153ro', secret_access_key = 'c05153secret')"
${CLICKHOUSE_CURL} -sS "$RO_URL" --data-binary "SET readonly = 1"

${CLICKHOUSE_CURL} -sS "$RO_URL" \
    --data-binary "SET custom_x = disk(type = 's3', name = 'c05153ro', secret_access_key = 'c05153secret')"
echo 'identical value accepted under readonly'

${CLICKHOUSE_CURL} -sS "$RO_URL" \
    --data-binary "SET custom_x = disk(type = 's3', name = 'c05153ro', secret_access_key = 'c05153other')" 2>&1 \
    | grep -c "Cannot modify 'custom_x' setting in readonly mode"
