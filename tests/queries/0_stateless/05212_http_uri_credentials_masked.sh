#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A password embedded in an HTTP URI must never appear in an error message, in SHOW CREATE output,
# or in system.query_log. Only the masked form scheme://user:[HIDDEN]@host may be shown.

PW="pwleakprobe9f2a"
URI="http://leakuser:${PW}@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/ping"

# Reads text from stdin and asserts it hides the password: no cleartext, and the masked marker present.
assert_masked() {
    local label="$1" text
    text=$(cat)
    if echo "$text" | grep -qF "$PW"; then
        echo "$label: FAIL cleartext password"
    elif echo "$text" | grep -qF '[HIDDEN]'; then
        echo "$label: OK masked"
    else
        echo "$label: FAIL no uri shown"
    fi
}

# 1. url() table function: the /ping response ("Ok.") fails to parse as CSV, so the URI is appended to
#    the exception as "(in file/uri ...)". Grep that line out of the exception - the client also echoes
#    the user's own submitted query, which legitimately contains what the user typed.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('${URI}', 'CSV', 'id UInt64, val String')" 2>&1 \
    | grep -F 'in file/uri' | assert_masked "url_function"

# 2. A dictionary whose HTTP source URL carries credentials.
${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS dict_uri_leak"
${CLICKHOUSE_CLIENT} --query "CREATE DICTIONARY dict_uri_leak (id UInt64, val String) PRIMARY KEY id SOURCE(HTTP(url '${URI}' format 'CSV')) LAYOUT(FLAT(SIZE_IN_CELLS 100)) LIFETIME(0)"

# 2a. Reloading fails to parse; the URI must be masked in the exception.
${CLICKHOUSE_CLIENT} --query "SYSTEM RELOAD DICTIONARY dict_uri_leak" 2>&1 \
    | grep -F 'in file/uri' | assert_masked "dictionary_reload"

# 2b. SHOW CREATE must mask the password.
${CLICKHOUSE_CLIENT} --query "SHOW CREATE DICTIONARY dict_uri_leak" 2>&1 | assert_masked "show_create"

# 3. system.query_log must store neither the query text nor the exception with the cleartext password.
#    The needle is split so that this checking query does not itself contain the contiguous secret.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} --query "
    SELECT 'query_log_cleartext', count()
    FROM system.query_log
    WHERE event_date >= today()
      AND current_database = currentDatabase()
      AND (query LIKE '%' || 'pwleakprobe' || '9f2a%' OR exception LIKE '%' || 'pwleakprobe' || '9f2a%')"

${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS dict_uri_leak"
