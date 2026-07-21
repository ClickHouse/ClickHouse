#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Base URL for the user-facing HTTP port (no path / no auth: default user).
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
P="/hm_${DB}"
HGET="hget_${DB}"
HPUT="hput_${DB}"
HDEL="hdel_${DB}"
HTMP="htmp_${DB}"
HTMPV="htmpv_${DB}"
HTAB="htab_${DB}"

cleanup() {
    local drops=""
    for h in "$HGET" "$HPUT" "$HDEL" "$HTMP" "$HTMPV" "$HTAB"; do
        drops+="DROP HANDLER IF EXISTS \`$h\`; "
    done
    $CLICKHOUSE_CLIENT -q "${drops}"
}
trap cleanup EXIT
cleanup

echo "=== a default (GET) handler answers HEAD with the same 200 status and no body ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HGET\` URL '${P}/get' AS SELECT 1 AS a FORMAT TSV"
# --head sends a HEAD request. A default GET handler must answer it (like config-defined handlers do),
# returning 200 and an empty body.
${CLICKHOUSE_CURL} -sS --head "${BASE}${P}/get" | grep -c '200 OK'

echo "=== an OPTIONS request to a handler endpoint gets the generic preflight response (204) ==="
${CLICKHOUSE_CURL} -sS -X OPTIONS -I "${BASE}${P}/get" | grep -c '204 No Content'

echo "=== a lengthless non-chunked PUT to a mutating handler is rejected with 411 ==="
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t (x UInt32) ENGINE = Memory; CREATE HANDLER \`$HPUT\` URL '${P}/put' METHODS (PUT) AS INSERT INTO ${DB}.t FORMAT TSV"
# `-X PUT -I` sends a PUT with neither Content-Length nor chunked Transfer-Encoding: without the guard
# the body would be read until EOF and a dropped connection accepted as a partial INSERT.
${CLICKHOUSE_CURL} -sS -X PUT -I "${BASE}${P}/put" | grep -c '411 Length Required'

echo "=== a lengthless non-chunked DELETE to a mutating handler is rejected with 411 ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HDEL\` URL '${P}/del' METHODS (DELETE) AS INSERT INTO ${DB}.t FORMAT TSV"
${CLICKHOUSE_CURL} -sS -X DELETE -I "${BASE}${P}/del" | grep -c '411 Length Required'

echo "=== CREATE TEMPORARY TABLE is allowed over a read-only (GET) handler method ==="
# CREATE TEMPORARY TABLE needs only CREATE_TEMPORARY_TABLE, which is allowed under readonly = 2 (the mode
# GET sets), so the create-time mutating-method gate must not reject it.
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HTMP\` URL '${P}/tmp' AS CREATE TEMPORARY TABLE tt (x UInt8) ENGINE = Memory" && echo "temp table handler created"

echo "=== CREATE TEMPORARY VIEW is allowed over a read-only (GET) handler method ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HTMPV\` URL '${P}/tmpv' AS CREATE TEMPORARY VIEW tv AS SELECT 1" && echo "temp view handler created"

echo "=== a non-temporary CREATE TABLE still requires a mutating handler method ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HTAB\` URL '${P}/tab' AS CREATE TABLE ${DB}.persistent (x UInt8) ENGINE = Memory" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1
