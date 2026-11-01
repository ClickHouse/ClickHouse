#!/usr/bin/env bash

# A custom setting has no default of its own, so `system.settings.default` repeats its value. That
# value can be an AST, e.g. `custom_x = disk(type = 's3', secret_access_key = '...')`, and only
# `CustomType::toString(show_secrets)` hides the credential in it, so the `default` column has to ask
# for the hidden form just like the `value` column does.
#
# `custom_x` only exists for the lifetime of an HTTP session, so the setting is set with
# `SET custom_x = ...` inside a session and the session is reused for the query that follows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CANARY="c05137customdefault"
SESSION="05137_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION" \
    --data-binary "SET custom_x = disk(type = 's3', secret_access_key = '$CANARY')" > /dev/null

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION" \
    --data-binary "SELECT
        position(default, '[HIDDEN]') > 0,
        position(default, '$CANARY') = 0
    FROM system.settings WHERE name = 'custom_x'"
