#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A credential embedded in an HTTP URI must never appear in an error message, in SHOW CREATE output,
# or in system.query_log. Only the masked form scheme://[HIDDEN]@host (or X-Amz-Signature=[HIDDEN] for
# a presigned URL) may be shown.

PW="pwleakprobe9f2a"
URI="http://leakuser:${PW}@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/ping"
USERINFO_SHAPE="[HIDDEN]@${CLICKHOUSE_HOST}"

# Reads text from stdin and asserts the credential was present but masked: the cleartext secret is
# absent and the required masked shape is present. Asserting the shape (not just that "[HIDDEN]" appears
# somewhere) means a text that never carried the credential fails, so the check cannot pass vacuously.
assert_shape() {
    local label="$1" secret="$2" shape="$3" text
    text=$(cat)
    if echo "$text" | grep -qF "$secret"; then
        echo "$label: FAIL cleartext"
    elif echo "$text" | grep -qF "$shape"; then
        echo "$label: OK masked"
    else
        echo "$label: FAIL uri absent"
    fi
}

# 1. url() table function: the /ping response ("Ok.") fails to parse as CSV, so the URI is appended to
#    the exception as "(in file/uri ...)". Grep that line out of the exception - the client also echoes
#    the user's own submitted query, which legitimately contains what the user typed.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('${URI}', 'CSV', 'id UInt64, val String')" 2>&1 \
    | grep -F 'in file/uri' | assert_shape "url_function" "$PW" "$USERINFO_SHAPE"

# 1b. An HTTP status failure (non-2xx) is reported by assertResponseIsOk as "Received error from
#     remote server <uri>", a different code path than the CSV-parse suffix above. A request to an
#     unknown path returns 404, so the URI in that exception must also be masked.
URI_404="http://leakuser:${PW}@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/no_such_handler_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('${URI_404}', 'CSV', 'id UInt64, val String')" 2>&1 \
    | grep -F 'Received error from remote server' | assert_shape "url_status_failure" "$PW" "$USERINFO_SHAPE"

# 1c. INSERT INTO url() writes through WriteBufferFromHTTP - the only path here that does - and a
#     presigned URL carries its credential in the query parameters, not the userinfo. A request with a
#     missing ?database returns 404 whose body does not echo the URI, so the signature can only appear
#     through the URI in the exception, which must be masked to X-Amz-Signature=[HIDDEN].
SIG="sigprobe${CLICKHOUSE_TEST_UNIQUE_NAME}"
URI_SIG="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?database=no_such_db_${CLICKHOUSE_TEST_UNIQUE_NAME}&X-Amz-Signature=${SIG}"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE FUNCTION url('${URI_SIG}', 'CSV', 'id UInt64') VALUES (1)" 2>&1 \
    | grep -F 'Received error from remote server' | assert_shape "insert_presigned" "$SIG" "X-Amz-Signature=[HIDDEN]"

# 2. A dictionary whose HTTP source URL carries credentials.
${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS dict_uri_leak"
${CLICKHOUSE_CLIENT} --query "CREATE DICTIONARY dict_uri_leak (id UInt64, val String) PRIMARY KEY id SOURCE(HTTP(url '${URI}' format 'CSV')) LAYOUT(FLAT(SIZE_IN_CELLS 100)) LIFETIME(0)"

# 2a. Reloading fails to parse; the URI must be masked in the exception.
${CLICKHOUSE_CLIENT} --query "SYSTEM RELOAD DICTIONARY dict_uri_leak" 2>&1 \
    | grep -F 'in file/uri' | assert_shape "dictionary_reload" "$PW" "$USERINFO_SHAPE"

# 2b. SHOW CREATE must mask the password.
${CLICKHOUSE_CLIENT} --query "SHOW CREATE DICTIONARY dict_uri_leak" 2>&1 | assert_shape "show_create" "$PW" "$USERINFO_SHAPE"

# 3. system.query_log must store neither the query text nor the exception with the cleartext password.
#    The needle is split so that this checking query does not itself contain the contiguous secret.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT 'query_log_cleartext', count()
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND current_database = currentDatabase()
      AND (query LIKE '%' || 'pwleakprobe' || '9f2a%' OR exception LIKE '%' || 'pwleakprobe' || '9f2a%')"

# 4. The url() table function feeds its query text into system.query_log through the same sanitizer.
#    It must mask the shapes the old password-only masker missed: a userinfo password that itself
#    contains '@' (masked whole, not just up to the first '@'), a bare userinfo token with no password,
#    and presigned-URL signature parameters. Run one query of each shape at a distinctive path, then
#    check that query_log logged them all masked and stored none of the cleartext secrets. The probes
#    are split in the checking queries so those queries do not themselves carry the contiguous secret.
PP="urlprobe_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://leakuser:first@atprobe7k3@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/${PP}', 'CSV', 'id UInt64')" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://tokprobe5x9@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/${PP}', 'CSV', 'id UInt64')" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/${PP}?X-Amz-Signature=sigprobe3q8', 'CSV', 'id UInt64')" >/dev/null 2>&1

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT 'url_query_log_masked', count() >= 3
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND current_database = currentDatabase()
      AND query LIKE '%' || 'urlprobe_' || '${CLICKHOUSE_TEST_UNIQUE_NAME}%'
      AND query LIKE '%[HIDDEN]%'"
${CLICKHOUSE_CLIENT} --query "
    SELECT 'url_query_log_cleartext', count()
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND current_database = currentDatabase()
      AND (query LIKE '%' || 'atprobe' || '7k3%' OR exception LIKE '%' || 'atprobe' || '7k3%'
           OR query LIKE '%' || 'tokprobe' || '5x9%' OR exception LIKE '%' || 'tokprobe' || '5x9%'
           OR query LIKE '%' || 'sigprobe' || '3q8%' OR exception LIKE '%' || 'sigprobe' || '3q8%')"

${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS dict_uri_leak"
