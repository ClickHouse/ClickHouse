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
HCONST="hconst_${DB}"
HSECRET="hsecret_${DB}"
HVIEW="hview_${DB}"
HWHO="hwho_${DB}"
HDBNAME="hdbname_${DB}"
HHDR="hhdr_${DB}"
HBRANCH="hbranch_${DB}"
HORDA="aaa_ord_${DB}"
HORDZ="zzz_ord_${DB}"
USER="u_${DB}"
RUSER="ru_${DB}"

cleanup() {
    for h in "$HA" "$HB" "$HC" "$HP" "$HPOST" "$HINS" "$HPROTO" "$HCONST" "$HSECRET" "$HVIEW" "$HWHO" "$HDBNAME" "$HHDR" "$HBRANCH" "$HORDA" "$HORDZ" "hx_${DB}"; do
        $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$h\`"
    done
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS \`$USER\`, \`$RUSER\`"
}
trap cleanup EXIT
cleanup

echo "=== exact URL, GET ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HA\` URL '${P}/exact' AS SELECT 1 AS a, 'hello' AS b FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact"

echo "=== URL match ignores query string ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact?max_block_size=100"

echo "=== URL PREFIX ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HP\` URL PREFIX '${P}/prefix/' AS SELECT 'prefixed' AS r FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/prefix/anything/here"

echo "=== currentHandler() and currentRequestURL() ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HB\` URL '${P}/introspect' AS SELECT currentHandler() = '${HB}' AS h_ok, currentRequestURL() = '${P}/introspect?max_block_size=100' AS u_ok FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/introspect?max_block_size=100"

echo "=== currentHandler() can be used to branch query behavior ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HBRANCH\` URL '${P}/branch' AS SELECT if(currentHandler() = '${HBRANCH}', 'matched', 'no') AS r FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/branch"

echo "=== parameterized query via regexp URL capture ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HC\` URL REGEXP '${P}/item/(?P<id>[0-9]+)' AS SELECT {id:UInt32} AS id FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/item/12345"

echo "=== parameterized query via URL query-string parameter ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HPOST\` URL '${P}/param' METHODS (GET, POST) AS SELECT {n:UInt32} * 2 AS doubled FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/param?param_n=21"

echo "=== method not allowed (GET-only handler, POST request) does not match ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact" --data-binary '' 2>/dev/null | grep -c 'hello'

echo "=== POST allowed when listed in METHODS ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/param?param_n=5" --data-binary ''

echo "=== INSERT handler reads data from the HTTP body ==="
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t (x UInt32) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HINS\` URL '${P}/insert' METHODS (POST) AS INSERT INTO ${DB}.t FORMAT TSV"
printf '1\n2\n3\n' | ${CLICKHOUSE_CURL} -sS "${BASE}${P}/insert" --data-binary @-
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM ${DB}.t"

echo "=== custom HTTP response headers via the SETTINGS of the query ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HHDR\` URL '${P}/rheaders' AS SELECT 1 SETTINGS http_response_headers = {'X-Custom':'yes'}"
${CLICKHOUSE_CURL} -sS -D - "${BASE}${P}/rheaders" -o /dev/null | grep -i '^x-custom' | tr -d '\r' | tr 'A-Z' 'a-z'

echo "=== HTTP header used as usual: X-ClickHouse-Database sets the database ==="
$CLICKHOUSE_CLIENT -q "CREATE DATABASE IF NOT EXISTS db2_${DB}"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HDBNAME\` URL '${P}/curdb' AS SELECT currentDatabase() = 'db2_${DB}' AS ok FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/curdb" -H "X-ClickHouse-Database: db2_${DB}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS db2_${DB}"

echo "=== SQL-defined handlers matched in lexicographic order of names ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HORDZ\` URL REGEXP '${P}/order/.*' AS SELECT 'Z' FORMAT TSV"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HORDA\` URL REGEXP '${P}/order/.*' AS SELECT 'A' FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/order/x"

echo "=== PROTOCOL-scoped handler is not served on the default http port ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HPROTO\` PROTOCOL some_other_protocol URL '${P}/proto' AS SELECT 'should_not_match' FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/proto" | grep -c 'should_not_match'

echo "=== query_log records handler name and request URL ==="
QID="q_${DB}_$RANDOM"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact?query_id=${QID}" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "SELECT http_handler_name = '${HA}' AS name_ok, http_request_url LIKE '%/exact%' AS url_ok FROM system.query_log WHERE query_id = '${QID}' AND type = 'QueryFinish' ORDER BY event_time DESC LIMIT 1"

echo "=== authentication: credentials provided in the request ==="
$CLICKHOUSE_CLIENT -q "CREATE USER \`$RUSER\` IDENTIFIED WITH plaintext_password BY 'pw'"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HWHO\` URL '${P}/whoami' AS SELECT currentUser() = '${RUSER}' AS ok FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/whoami?user=${RUSER}&password=pw"

echo "=== invoking a handler needs no special grant; SELECT of a constant works for any user ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HCONST\` URL '${P}/const' AS SELECT 42 FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/const?user=${RUSER}&password=pw"

echo "=== grants are checked as usual during invocation: no access to a table -> denied ==="
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.secret (x UInt32) ENGINE = Memory AS SELECT 111"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HSECRET\` URL '${P}/secret' AS SELECT x FROM ${DB}.secret FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/secret?user=${RUSER}&password=pw" | grep -oE 'ACCESS_DENIED|111' | head -1

echo "=== SQL SECURITY DEFINER view lets a restricted user read through a handler ==="
$CLICKHOUSE_CLIENT -q "CREATE VIEW ${DB}.sv DEFINER=default SQL SECURITY DEFINER AS SELECT x FROM ${DB}.secret"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${DB}.sv TO \`$RUSER\`"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HVIEW\` URL '${P}/view' AS SELECT x FROM ${DB}.sv FORMAT TSV"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/view?user=${RUSER}&password=pw"

echo "=== access control: CREATE/ALTER/DROP HANDLER require separate grants ==="
$CLICKHOUSE_CLIENT -q "CREATE USER \`$USER\` IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --user "$USER" -q "CREATE HANDLER \`hx_${DB}\` URL '${P}/denied' AS SELECT 1" 2>&1 | grep -o "ACCESS_DENIED" | head -1
$CLICKHOUSE_CLIENT -q "GRANT CREATE HANDLER ON *.* TO \`$USER\`"
$CLICKHOUSE_CLIENT --user "$USER" -q "CREATE HANDLER \`hx_${DB}\` URL '${P}/granted' AS SELECT 1" && echo "create with grant ok"
# CREATE HANDLER grant does not imply ALTER HANDLER or DROP HANDLER.
$CLICKHOUSE_CLIENT --user "$USER" -q "ALTER HANDLER \`hx_${DB}\` AS SELECT 2" 2>&1 | grep -o "ACCESS_DENIED" | head -1
$CLICKHOUSE_CLIENT --user "$USER" -q "DROP HANDLER \`hx_${DB}\`" 2>&1 | grep -o "ACCESS_DENIED" | head -1
$CLICKHOUSE_CLIENT -q "GRANT ALTER HANDLER ON *.* TO \`$USER\`"
$CLICKHOUSE_CLIENT --user "$USER" -q "ALTER HANDLER \`hx_${DB}\` AS SELECT 2" && echo "alter with grant ok"
$CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`hx_${DB}\`"
