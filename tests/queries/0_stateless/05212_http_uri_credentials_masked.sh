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
#    Print the offending rows rather than a count: on success this selects nothing (the reference has
#    no line here), and any row printed on failure is the culprit - its query and exception show what
#    leaked. The needle is split so that this checking query does not itself contain the contiguous
#    secret.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT type, query, exception
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND current_database = currentDatabase()
      AND (query LIKE '%' || 'pwleakprobe' || '9f2a%' OR exception LIKE '%' || 'pwleakprobe' || '9f2a%')
    FORMAT Vertical"

# 4. The url() table function feeds its query text into system.query_log through the same sanitizer.
#    It must mask the shapes the old password-only masker missed: a userinfo password that itself
#    contains '@' (masked whole, not just up to the first '@'), a bare userinfo token with no password,
#    and presigned-URL signature parameters. Run one query of each shape, then check that query_log
#    logged them all masked and stored none of the cleartext secrets - in the query text or the
#    exception. The marker that makes the probes findable has to survive masking, so it is a column
#    name in the structure (the userinfo and the presigned parameters are masked, a column name is
#    not). The two userinfo probes point at /ping, which needs no auth and answers "Ok.": the row then
#    fails to parse locally, so the remote server never auth-fails and echoes no userinfo back into the
#    exception. The probes are split in the checking queries so those queries do not themselves carry
#    the contiguous secret.
PP="urlprobe_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://leakuser:first@atprobe7k3@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/ping', 'CSV', '${PP} UInt64')" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://tokprobe5x9@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/ping', 'CSV', '${PP} UInt64')" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/ping?X-Amz-Signature=sigprobe3q8', 'CSV', '${PP} UInt64')" >/dev/null 2>&1

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT 'url_query_log_masked', count() >= 3
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND current_database = currentDatabase()
      AND query LIKE '%' || 'urlprobe_' || '${CLICKHOUSE_TEST_UNIQUE_NAME}%'
      AND query LIKE '%[HIDDEN]%'"
# On success this selects nothing; any row printed on failure is the culprit, and its query and
# exception columns show exactly what leaked and where. The needles are split so this checking query
# does not itself carry a contiguous secret.
${CLICKHOUSE_CLIENT} --query "
    SELECT type, query, exception
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND current_database = currentDatabase()
      AND (query LIKE '%' || 'atprobe' || '7k3%' OR exception LIKE '%' || 'atprobe' || '7k3%'
           OR query LIKE '%' || 'tokprobe' || '5x9%' OR exception LIKE '%' || 'tokprobe' || '5x9%'
           OR query LIKE '%' || 'sigprobe' || '3q8%' OR exception LIKE '%' || 'sigprobe' || '3q8%')
    FORMAT Vertical"

# 5. Schema inference stores the source URL in system.schema_inference_cache; the credential in its
#    userinfo must be masked there too. Inferring a schema from a credentialed /ping URL (which needs
#    no auth and whose "Ok." body is inferrable as one column) populates the cache.
SIC_SECRET="sicpwprobe${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://leakuser:${SIC_SECRET}@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/ping', 'CSV')" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT source FROM system.schema_inference_cache WHERE storage = 'URL'" \
    | assert_shape "schema_inference_cache" "$SIC_SECRET" "[HIDDEN]@${CLICKHOUSE_HOST}"

# 6. A header value is never decoded as a credential. An 'Authorization: Basic <payload>' whose payload
#    is not valid base64 (here supplied through headers()) must reach the endpoint verbatim, not fail
#    while the request is inspected for secrets to scrub. /ping needs no auth and answers "Ok.".
A_OUT=$(${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/ping', 'LineAsString', 'line String', headers('Authorization' = 'Basic ghp_AbCd1234'))" 2>&1)
if echo "$A_OUT" | grep -qF 'Ok.'; then echo "basic_header_passthrough: OK"; else echo "basic_header_passthrough: FAIL"; fi

# 7. A URL userinfo is sent as 'Authorization: Basic base64(user:password)'. Recovering the user name
#    from that header to scrub the error body would replace a one-letter user ('a') everywhere it
#    occurs, turning the remote "Authentication failed" body into "Authentic[HIDDEN]tion f[HIDDEN]iled".
#    The body must be preserved, and the password must not appear. Authenticating against ClickHouse's
#    own HTTP port with a bad password returns that body. Grep the "Received error from remote server"
#    line out of the exception: the client also echoes the user's own submitted query, which
#    legitimately contains the password the user typed.
B_OUT=$(${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://a:${PW}@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'CSV', 'x UInt8')" 2>&1 \
    | grep -F 'Received error from remote server')
if echo "$B_OUT" | grep -qF "$PW"; then
    echo "body_not_overmasked: FAIL cleartext"
elif echo "$B_OUT" | grep -qF 'Authentication failed'; then
    echo "body_not_overmasked: OK"
else
    echo "body_not_overmasked: FAIL mangled"
fi

${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS dict_uri_leak"
