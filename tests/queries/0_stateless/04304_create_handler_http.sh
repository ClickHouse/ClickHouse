#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Base URL for the user-facing HTTP port (no path / no auth: default user).
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
P="/h_${DB}"
HA="ha_${DB}"
HB="hb_${DB}"
HC="hc_${DB}"
HP="hp_${DB}"
HPOST="hpost_${DB}"
HINS="hins_${DB}"
HPROTO="hproto_${DB}"

cleanup() {
    for h in "$HA" "$HB" "$HC" "$HP" "$HPOST" "$HINS" "$HPROTO"; do
        $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$h\`"
    done
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS u_${DB}"
}
trap cleanup EXIT
cleanup

echo "=== exact URL, GET ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HA\` URL '${P}/exact' AS SELECT 1 AS a, 'hello' AS b FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact"

echo "=== URL match ignores query string and fragment ==="
# A recognized setting is accepted in the query string; the path still matches the handler.
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact?max_block_size=100"

echo "=== URL PREFIX ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HP\` URL PREFIX '${P}/prefix/' AS SELECT 'prefixed' AS r FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/prefix/anything/here"

echo "=== currentHandler() and currentRequestURL() ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HB\` URL '${P}/introspect' AS SELECT currentHandler() = '${HB}' AS h_ok, currentRequestURL() = '${P}/introspect?max_block_size=100' AS u_ok FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/introspect?max_block_size=100"

echo "=== parameterized query via regexp URL capture ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HC\` URL REGEXP '${P}/item/(?P<id>[0-9]+)' AS SELECT {id:UInt32} AS id FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/item/12345"

echo "=== parameterized query via URL query-string parameter ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HPOST\` URL '${P}/param' METHODS (GET, POST) AS SELECT {n:UInt32} * 2 AS doubled FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/param?param_n=21"

echo "=== method not allowed (GET-only handler, POST request) does not match ==="
# POST to the GET-only handler must not run it (count of its output 'hello' must be 0).
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact" --data-binary '' 2>/dev/null | grep -c 'hello'

echo "=== POST allowed ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/param?param_n=5" --data-binary ''

echo "=== INSERT handler reads data from the HTTP body ==="
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t (x UInt32) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HINS\` URL '${P}/insert' METHODS (POST) AS INSERT INTO ${DB}.t FORMAT TSV"
printf '1\n2\n3\n' | ${CLICKHOUSE_CURL} -sS "${BASE}${P}/insert" --data-binary @-
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM ${DB}.t"

echo "=== PROTOCOL-scoped handler is not served on the default http port ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HPROTO\` PROTOCOL some_other_protocol URL '${P}/proto' AS SELECT 'should_not_match' FORMAT TSV"
# Should not match -> default handler reports it could not find the path (non-empty error, not 'should_not_match').
${CLICKHOUSE_CURL} -sS "${BASE}${P}/proto" | grep -c 'should_not_match'

echo "=== query_log records handler name and request URL ==="
QID="q_${DB}_$RANDOM"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact?query_id=${QID}" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "SELECT http_handler_name = '${HA}' AS name_ok, http_request_url LIKE '%/exact%' AS url_ok FROM system.query_log WHERE query_id = '${QID}' AND type = 'QueryFinish' ORDER BY event_time DESC LIMIT 1"

echo "=== access control: CREATE HANDLER requires a grant ==="
$CLICKHOUSE_CLIENT -q "CREATE USER u_${DB} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --user "u_${DB}" -q "CREATE HANDLER \`hx_${DB}\` URL '${P}/denied' AS SELECT 1" 2>&1 | grep -o "ACCESS_DENIED" | head -1
$CLICKHOUSE_CLIENT -q "GRANT CREATE HANDLER ON *.* TO u_${DB}"
$CLICKHOUSE_CLIENT --user "u_${DB}" -q "CREATE HANDLER \`hx_${DB}\` URL '${P}/granted' AS SELECT 1" && echo "create with grant ok"
$CLICKHOUSE_CLIENT --user "u_${DB}" -q "DROP HANDLER IF EXISTS \`hx_${DB}\`" 2>&1 | grep -o "ACCESS_DENIED" | head -1
$CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`hx_${DB}\`"
