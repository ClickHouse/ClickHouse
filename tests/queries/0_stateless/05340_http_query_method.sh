#!/usr/bin/env bash

# The HTTP `QUERY` method (a safe method with a request body): the query is sent in the body like with `POST`,
# but the request is readonly like `GET`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
SQL='Content-Type: application/sql'
HANDLER="h_05340_${CLICKHOUSE_DATABASE}"
HANDLER_URL="/h_05340_${CLICKHOUSE_DATABASE}"
HANDLER_BODY="hb_05340_${CLICKHOUSE_DATABASE}"
HANDLER_BODY_URL="/hb_05340_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS \`${HANDLER}\`; DROP HANDLER IF EXISTS \`${HANDLER_BODY}\`; DROP HANDLER IF EXISTS \`${HANDLER}_bad\`"
}
trap cleanup EXIT
cleanup

echo "--- query in the body"
echo 'SELECT 1' | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" "${CLICKHOUSE_URL}" --data-binary @-
echo 'SELECT 1' | ${CLICKHOUSE_CURL} -sS -X QUERY -H 'Content-Type: text/plain; charset=utf-8' "${CLICKHOUSE_URL}" --data-binary @-

echo "--- beginning of the query in the URL, the rest in the body"
echo '+ 1' | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" "${CLICKHOUSE_URL}&query=SELECT+1" --data-binary @-

echo "--- chunked body"
echo 'SELECT 2' | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" -H 'Transfer-Encoding: chunked' "${CLICKHOUSE_URL}" --data-binary @-

echo "--- /query path"
echo 'SELECT 3' | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" "${BASE}/query?${CLICKHOUSE_URL_PARAMS}" --data-binary @-
echo 'SELECT 3' | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" "${BASE}/query" --data-binary @-

echo "--- table path"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_http_query_method (x UInt8) ENGINE = Memory AS SELECT number FROM numbers(4)"
echo 'SELECT sum(x)' | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" "${BASE}/${CLICKHOUSE_DATABASE}/t_http_query_method" --data-binary @-

echo "--- readonly = 2, settings can be changed"
echo "SELECT getSetting('readonly'), getSetting('max_threads')" | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" "${CLICKHOUSE_URL}&max_threads=3" --data-binary @-

echo "--- modifying queries are rejected"
echo 'INSERT INTO t_http_query_method VALUES (1)' | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" "${CLICKHOUSE_URL}" --data-binary @- | grep -o 'READONLY'
echo 'CREATE TABLE t_http_query_method_2 (x UInt8) ENGINE = Memory' | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" "${CLICKHOUSE_URL}" --data-binary @- | grep -o 'READONLY'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_http_query_method"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_http_query_method_2"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_http_query_method"

echo "--- external data in a multipart body"
printf '1\n2\n3\n' | ${CLICKHOUSE_CURL} -sS -X QUERY -F 'ext=@-' "${CLICKHOUSE_URL}&ext_structure=x+UInt8&query=SELECT+sum(x)+FROM+ext"

echo "--- no Content-Length and not chunked"
${CLICKHOUSE_CURL} -sS -X QUERY -o /dev/null -w '%{http_code}\n' "${CLICKHOUSE_URL}&query=SELECT+1"

echo "--- missing or unsupported media type"
echo 'SELECT 1' | ${CLICKHOUSE_CURL} -sS -X QUERY -H 'Content-Type:' -o /dev/null -w '%{http_code}\n' "${CLICKHOUSE_URL}" --data-binary @-
echo 'SELECT 1' | ${CLICKHOUSE_CURL} -sS -X QUERY -H 'Content-Type: application/json' -o /dev/null -w '%{http_code}\n' "${CLICKHOUSE_URL}" --data-binary @-
echo 'SELECT 1' | ${CLICKHOUSE_CURL} -sS -X QUERY -o /dev/null -w '%{http_code}\n' "${CLICKHOUSE_URL}" --data-binary @-

echo "--- unknown path"
echo 'SELECT 1' | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" -o /dev/null -w '%{http_code}\n' "${BASE}/this/path/does/not/exist/05340" --data-binary @-

echo "--- CORS preflight"
# `Content-Type` is allowed only in a preflight for `QUERY`, the preflight for other methods is unchanged.
for method in QUERY POST; do
    ${CLICKHOUSE_CURL} -sS -X OPTIONS -H 'Origin: http://example.com' -H "Access-Control-Request-Method: ${method}" -D - -o /dev/null "${CLICKHOUSE_URL}" \
        | grep -i -E '^Access-Control-Allow-(Methods|Headers):' | tr -d '\r' | sort
done

echo "--- SQL-defined handler with form parameters in the body"
${CLICKHOUSE_CLIENT} --query "CREATE HANDLER \`${HANDLER}\` URL '${HANDLER_URL}' METHODS (QUERY) AS SELECT {n:UInt32} * 2 FORMAT TSV"
${CLICKHOUSE_CURL} -sS -X QUERY -H 'Content-Type: application/x-www-form-urlencoded' "${BASE}${HANDLER_URL}" --data-binary 'n=7'
${CLICKHOUSE_CURL} -sS -X QUERY -F 'n=8' "${BASE}${HANDLER_URL}"
${CLICKHOUSE_CLIENT} --query "SELECT methods FROM system.handlers WHERE name = '${HANDLER}'"

echo "--- SQL-defined handler reading the raw request body"
${CLICKHOUSE_CLIENT} --query "CREATE HANDLER \`${HANDLER_BODY}\` URL '${HANDLER_BODY_URL}' METHODS (POST) AS SELECT {_request_body:String} FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "ALTER HANDLER \`${HANDLER_BODY}\` METHODS (POST, QUERY)"
${CLICKHOUSE_CURL} -sS -X QUERY -H 'Content-Type: text/plain' "${BASE}${HANDLER_BODY_URL}" --data-binary 'hello'
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${HANDLER_BODY_URL}" --data-binary 'world'
${CLICKHOUSE_CLIENT} --query "DROP HANDLER \`${HANDLER_BODY}\`"
${CLICKHOUSE_CLIENT} --query "CREATE HANDLER \`${HANDLER_BODY}\` URL '${HANDLER_BODY_URL}' METHODS (QUERY) AS SELECT {_request_body:String} FORMAT TSV"
${CLICKHOUSE_CURL} -sS -X QUERY -H 'Content-Type: text/plain' "${BASE}${HANDLER_BODY_URL}" --data-binary 'hello again'

echo "--- a method without a body still cannot read the body, and QUERY still cannot modify data"
${CLICKHOUSE_CLIENT} --query "CREATE HANDLER \`${HANDLER}_bad\` URL '${HANDLER_URL}_bad' METHODS (GET, QUERY) AS SELECT {_request_body:String}" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_http_query_method_handler (s String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "CREATE HANDLER \`${HANDLER}_bad\` URL '${HANDLER_URL}_bad' METHODS (QUERY) AS INSERT INTO ${CLICKHOUSE_DATABASE}.t_http_query_method_handler FORMAT LineAsString" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_http_query_method_handler"

echo "--- the method is reported in query_log"
QID="q_05340_${CLICKHOUSE_DATABASE}_$RANDOM"
echo 'SELECT 1' | ${CLICKHOUSE_CURL} -sS -X QUERY -H "${SQL}" "${CLICKHOUSE_URL}&query_id=${QID}" --data-binary @- > /dev/null
# Retry to handle the race between the HTTP response and the log entry being written.
for _ in {1..60}; do
    res=$(${CLICKHOUSE_CLIENT} -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT http_method FROM system.query_log
        WHERE event_date >= yesterday() AND query_id = '${QID}' AND type = 'QueryFinish' AND current_database = currentDatabase()")
    [ -n "$res" ] && break
    sleep 0.5
done
echo "$res"
