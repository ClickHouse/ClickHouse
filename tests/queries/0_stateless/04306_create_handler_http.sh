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
HPUT="hput_${DB}"
HDEL="hdel_${DB}"
HPROTO="hproto_${DB}"
HCONST="hconst_${DB}"
HSECRET="hsecret_${DB}"
HVIEW="hview_${DB}"
HWHO="hwho_${DB}"
HDBNAME="hdbname_${DB}"
HHDR="hhdr_${DB}"
HBRANCH="hbranch_${DB}"
HDIST="hdist_${DB}"
HORDA="aaa_ord_${DB}"
HORDZ="zzz_ord_${DB}"
USER="u_${DB}"
RUSER="ru_${DB}"

# This test runs under the flaky check (many repetitions) and under heavy configurations (sanitizers,
# S3 storage, metadata in Keeper), where starting `clickhouse-client` dominates the running time, so
# every statement that produces no output is batched into as few connections as possible.
cleanup() {
    local drops=""
    for h in "$HA" "$HB" "$HC" "$HP" "$HPOST" "$HINS" "$HPUT" "$HDEL" "$HPROTO" "$HCONST" "$HSECRET" "$HVIEW" "$HWHO" "$HDBNAME" "$HHDR" "$HBRANCH" "$HDIST" "$HORDA" "$HORDZ" "hbad_${DB}" "hx_${DB}"; do
        drops+="DROP HANDLER IF EXISTS \`$h\`; "
    done
    $CLICKHOUSE_CLIENT -q "${drops}DROP DATABASE IF EXISTS db2_${DB}; DROP USER IF EXISTS \`$USER\`, \`$RUSER\`"
}
trap cleanup EXIT
cleanup

# Everything the checks below need, in a single connection. The handlers match disjoint URLs, so
# creating them all up front is equivalent to creating each one right before it is invoked.
$CLICKHOUSE_CLIENT -q "
CREATE USER \`$RUSER\` IDENTIFIED WITH plaintext_password BY 'pw';
CREATE USER \`$USER\` IDENTIFIED WITH no_password;
CREATE DATABASE db2_${DB};
CREATE TABLE ${DB}.t (x UInt32) ENGINE = Memory;
CREATE TABLE ${DB}.secret (x UInt32) ENGINE = Memory AS SELECT 111;
CREATE VIEW ${DB}.sv DEFINER=default SQL SECURITY DEFINER AS SELECT x FROM ${DB}.secret;
GRANT SELECT ON ${DB}.sv TO \`$RUSER\`;
CREATE HANDLER \`$HA\` URL '${P}/exact' AS SELECT 1 AS a, 'hello' AS b FORMAT TSV;
CREATE HANDLER \`$HP\` URL PREFIX '${P}/prefix/' AS SELECT 'prefixed' AS r FORMAT TSV;
CREATE HANDLER \`$HB\` URL '${P}/introspect' AS SELECT currentHandler() = '${HB}' AS h_ok, currentRequestURL() = '${P}/introspect?max_block_size=100' AS u_ok FORMAT TSV;
CREATE HANDLER \`$HBRANCH\` URL '${P}/branch' AS SELECT if(currentHandler() = '${HBRANCH}', 'matched', 'no') AS r FORMAT TSV;
CREATE HANDLER \`$HDIST\` URL '${P}/dist' AS SELECT * FROM remote('127.0.0.2', view(SELECT currentHandler() = '${HDIST}' AS h_ok, currentRequestURL() = '${P}/dist' AS u_ok)) FORMAT TSV;
CREATE HANDLER \`$HC\` URL REGEXP '${P}/item/(?P<id>[0-9]+)' AS SELECT {id:UInt32} AS id FORMAT TSV;
CREATE HANDLER \`$HPOST\` URL '${P}/param' METHODS (GET, POST) AS SELECT {n:UInt32} * 2 AS doubled FORMAT TSV;
CREATE HANDLER \`$HHDR\` URL '${P}/rheaders' AS SELECT 1 SETTINGS http_response_headers = {'X-Custom':'yes'};
CREATE HANDLER \`$HDBNAME\` URL '${P}/curdb' AS SELECT currentDatabase() = 'db2_${DB}' AS ok FORMAT TSV;
CREATE HANDLER \`$HORDZ\` URL REGEXP '${P}/order/.*' AS SELECT 'Z' FORMAT TSV;
CREATE HANDLER \`$HORDA\` URL REGEXP '${P}/order/.*' AS SELECT 'A' FORMAT TSV;
CREATE HANDLER \`$HPROTO\` PROTOCOL some_other_protocol URL '${P}/proto' AS SELECT 'should_not_match' FORMAT TSV;
CREATE HANDLER \`$HWHO\` URL '${P}/whoami' AS SELECT currentUser() = '${RUSER}' AS ok FORMAT TSV;
CREATE HANDLER \`$HCONST\` URL '${P}/const' AS SELECT 42 FORMAT TSV;
CREATE HANDLER \`$HSECRET\` URL '${P}/secret' AS SELECT x FROM ${DB}.secret FORMAT TSV;
CREATE HANDLER \`$HVIEW\` URL '${P}/view' AS SELECT x FROM ${DB}.sv FORMAT TSV;
CREATE HANDLER \`$HPUT\` URL '${P}/insert_put' METHODS (PUT) AS INSERT INTO ${DB}.t FORMAT TSV"

# A handler whose query is an `INSERT ... FORMAT <name>` must be the last statement of a batch: the
# parser looks for inline `INSERT` data right after the format name, and a `;` there is an error.
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HDEL\` URL '${P}/insert_del' METHODS (DELETE) AS INSERT INTO ${DB}.t FORMAT TSV"
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HINS\` URL '${P}/insert' METHODS (POST) AS INSERT INTO ${DB}.t FORMAT TSV"

echo "=== exact URL, GET ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact"

echo "=== URL match ignores query string ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact?max_block_size=100"

echo "=== URL PREFIX ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/prefix/anything/here"

echo "=== currentHandler() and currentRequestURL() ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/introspect?max_block_size=100"

echo "=== currentHandler() can be used to branch query behavior ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/branch"

echo "=== currentHandler() and currentRequestURL() are visible on remote shards of a distributed query ==="
# The handler name and request URL live in ClientInfo, so they are serialized on distributed fan-out.
# The handler evaluates them on a remote shard via remote(view(...)) and compares them with the values
# seen locally.
${CLICKHOUSE_CURL} -sS "${BASE}${P}/dist"

echo "=== parameterized query via regexp URL capture ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/item/12345"

echo "=== parameterized query via URL query-string parameter ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/param?param_n=21"

echo "=== method not allowed (GET-only handler, POST request) does not match ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/exact" --data-binary '' 2>/dev/null | grep -c 'hello'

echo "=== POST allowed when listed in METHODS ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/param?param_n=5" --data-binary ''

echo "=== INSERT handler reads data from the HTTP body ==="
printf '1\n2\n3\n' | ${CLICKHOUSE_CURL} -sS "${BASE}${P}/insert" --data-binary @-
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM ${DB}.t"

echo "=== INSERT handler via the mutating method PUT writes data (not readonly) ==="
printf '10\n' | ${CLICKHOUSE_CURL} -sS -X PUT "${BASE}${P}/insert_put" --data-binary @-
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM ${DB}.t"

echo "=== INSERT handler via the mutating method DELETE writes data (not readonly) ==="
printf '100\n' | ${CLICKHOUSE_CURL} -sS -X DELETE "${BASE}${P}/insert_del" --data-binary @-
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM ${DB}.t"

echo "=== a modifying handler with only read-only methods is rejected at creation ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`hbad_${DB}\` URL '${P}/insert_bad' AS INSERT INTO ${DB}.t FORMAT TSV" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== custom HTTP response headers via the SETTINGS of the query ==="
${CLICKHOUSE_CURL} -sS -D - "${BASE}${P}/rheaders" -o /dev/null | grep -i '^x-custom' | tr -d '\r' | tr 'A-Z' 'a-z'

echo "=== HTTP header used as usual: X-ClickHouse-Database sets the database ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/curdb" -H "X-ClickHouse-Database: db2_${DB}"

echo "=== SQL-defined handlers matched in lexicographic order of names ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/order/x"

echo "=== PROTOCOL-scoped handler is not served on the default http port ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/proto" | grep -c 'should_not_match'

# The four requests whose `system.query_log` rows are checked below are made in one go and inspected
# with a single `SYSTEM FLUSH LOGS query_log`: flushing is the expensive part, so doing it once for all
# of them keeps the running time down. They are addressed to this test's database so that the logged
# rows have current_database = currentDatabase().
QID="q_${DB}_$RANDOM"
QIDP="qp_${DB}_$RANDOM"
QIDD="qd_${DB}_$RANDOM"
QIDH="qh_${DB}_$RANDOM"
${CLICKHOUSE_CURL} -sS -H "X-ClickHouse-Database: ${DB}" "${BASE}${P}/exact?query_id=${QID}" > /dev/null
printf '7\n' | ${CLICKHOUSE_CURL} -sS -X PUT -H "X-ClickHouse-Database: ${DB}" "${BASE}${P}/insert_put?query_id=${QIDP}" --data-binary @- > /dev/null
printf '8\n' | ${CLICKHOUSE_CURL} -sS -X DELETE -H "X-ClickHouse-Database: ${DB}" "${BASE}${P}/insert_del?query_id=${QIDD}" --data-binary @- > /dev/null
${CLICKHOUSE_CURL} -sS --head -H "X-ClickHouse-Database: ${DB}" "${BASE}${P}/exact?query_id=${QIDH}" > /dev/null
# Retry to handle the race between the HTTP responses and the log entries being written.
for _ in {1..60}; do
    res=$($CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT
            uniqExact(query_id) = 4 AS ready,
            anyIf(http_handler_name = '${HA}', query_id = '${QID}') AS name_ok,
            -- http_request_url must equal the path only: the query string (here \`?query_id=...\`) is not persisted.
            anyIf(http_request_url = '${P}/exact', query_id = '${QID}') AS url_is_path_only,
            -- PUT and DELETE are logged as http_method 4 and 5, and a HEAD request as 6 (not 0/UNKNOWN).
            anyIf(http_method, query_id = '${QIDP}') AS put_m,
            anyIf(http_method, query_id = '${QIDD}') AS del_m,
            anyIf(http_method, query_id = '${QIDH}') AS head_m
        FROM system.query_log
        WHERE query_id IN ('${QID}', '${QIDP}', '${QIDD}', '${QIDH}') AND type = 'QueryFinish' AND current_database = currentDatabase()")
    [ "${res%%$'\t'*}" = "1" ] && break
    sleep 0.5
done
read -r _ready name_ok url_is_path_only put_m del_m head_m <<< "$res"

echo "=== query_log records handler name and request path (query string stripped) ==="
printf '%s\t%s\n' "$name_ok" "$url_is_path_only"

echo "=== query_log records the HTTP method for PUT and DELETE handlers ==="
printf '%s\t%s\n' "$put_m" "$del_m"

echo "=== query_log records the HTTP method for a HEAD request served by a GET handler ==="
echo "$head_m"

echo "=== authentication: credentials provided in the request ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/whoami?user=${RUSER}&password=pw"

echo "=== invoking a handler needs no special grant; SELECT of a constant works for any user ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/const?user=${RUSER}&password=pw"

echo "=== grants are checked as usual during invocation: no access to a table -> denied ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/secret?user=${RUSER}&password=pw" | grep -oE 'ACCESS_DENIED|111' | head -1

echo "=== SQL SECURITY DEFINER view lets a restricted user read through a handler ==="
${CLICKHOUSE_CURL} -sS "${BASE}${P}/view?user=${RUSER}&password=pw"

echo "=== access control: CREATE/ALTER/DROP HANDLER require separate grants ==="
$CLICKHOUSE_CLIENT --user "$USER" -q "CREATE HANDLER \`hx_${DB}\` URL '${P}/denied' AS SELECT 1" 2>&1 | grep -o "ACCESS_DENIED" | head -1
$CLICKHOUSE_CLIENT -q "GRANT CREATE HANDLER ON *.* TO \`$USER\`"
$CLICKHOUSE_CLIENT --user "$USER" -q "CREATE HANDLER \`hx_${DB}\` URL '${P}/granted' AS SELECT 1" && echo "create with grant ok"
# CREATE HANDLER grant does not imply ALTER HANDLER or DROP HANDLER.
$CLICKHOUSE_CLIENT --user "$USER" -q "ALTER HANDLER \`hx_${DB}\` AS SELECT 2" 2>&1 | grep -o "ACCESS_DENIED" | head -1
$CLICKHOUSE_CLIENT --user "$USER" -q "DROP HANDLER \`hx_${DB}\`" 2>&1 | grep -o "ACCESS_DENIED" | head -1
$CLICKHOUSE_CLIENT -q "GRANT ALTER HANDLER ON *.* TO \`$USER\`"
$CLICKHOUSE_CLIENT --user "$USER" -q "ALTER HANDLER \`hx_${DB}\` AS SELECT 2" && echo "alter with grant ok"
