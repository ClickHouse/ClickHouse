#!/usr/bin/env bash
# Tags: no-fasttest, no-flaky-check

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A unique secret marker so we only look at log lines produced by this test (parallel-safe).
secret="find_me_${CLICKHOUSE_DATABASE}_TOPSECRET"
# `system.text_log` is keyed by time: its scans below are bounded to this test's lifetime.
test_start=$(${CLICKHOUSE_CLIENT} --query="SELECT toUnixTimestamp(now())")

# A SQL-defined handler with a receive parameter (handlers are global: the name and URL are unique to this test).
handler="h_04401_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query="DROP HANDLER IF EXISTS \`${handler}\`"
${CLICKHOUSE_CLIENT} --query="CREATE HANDLER \`${handler}\` URL '/${handler}' AS SELECT {v:String} = '' AS r FORMAT TSV"
trap '${CLICKHOUSE_CLIENT} --query="DROP HANDLER IF EXISTS \`${handler}\`"' EXIT

# Send the secret as the value of sensitive query-string parameters. The server logs
# "Request URI: ..." at trace level; the value must be redacted there.
#
# The query body is always "SELECT 1" and never references the bound parameter, so the only
# place the secret could surface is the URI log. (If the query echoed {secret_key:String},
# the substituted value would legitimately appear in the executeQuery log, which is a
# different code path unrelated to URI masking.) Each parameter uses a distinct suffix so the
# per-parameter assertions below cannot be confused with one another.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_secret_key=${secret}_skey" -d "SELECT 1 FORMAT Null" >/dev/null 2>&1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&password=${secret}_pw" -d "SELECT 1 FORMAT Null" >/dev/null 2>&1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&sig=${secret}_sig" -d "SELECT 1 FORMAT Null" >/dev/null 2>&1
# Percent-encoded sensitive name: the server decodes "pass%77ord" to "password" before using it,
# so masking must too, or the value leaks. --globoff stops curl treating the literal as a glob.
${CLICKHOUSE_CURL} -sS --globoff "${CLICKHOUSE_URL}&pass%77ord=${secret}_enc" -d "SELECT 1 FORMAT Null" >/dev/null 2>&1

# The query text and the bound query parameters are logged by executeQuery with secrets masked, so the
# Request URI line must hide them: a credential in the query, under an encoded name (quer%79 -> query),
# bound through a parameter, and in the clickhouse.uri attribute of the HTTPHandler span. Each request
# has its own query_id to find its Request URI line. EXPLAIN AST only parses: nothing connects to s3.
function s3_query()
{
    echo "EXPLAIN AST SELECT * FROM s3('http://127.0.0.1:1/x.csv', 'AKIAEXAMPLE', $1, 'CSV', 'a String')"
}
trace_id=$(${CLICKHOUSE_CLIENT} --query="SELECT lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CURL} -sS -G "${CLICKHOUSE_URL}&query_id=${secret}_id_a" \
    --data-urlencode "query=$(s3_query "'${secret}_qtext'")" >/dev/null 2>&1
${CLICKHOUSE_CURL} -sS -G --globoff "${CLICKHOUSE_URL}&query_id=${secret}_id_b" \
    --data-urlencode "quer%79=$(s3_query "'${secret}_qname'")" >/dev/null 2>&1
${CLICKHOUSE_CURL} -sS -G "${CLICKHOUSE_URL}&query_id=${secret}_id_c" \
    --data-urlencode "param_s=${secret}_qbound" --data-urlencode "query=$(s3_query '{s:String}')" >/dev/null 2>&1
${CLICKHOUSE_CURL} -sS -G "${CLICKHOUSE_URL}&query_id=${secret}_id_d" --header "traceparent: 00-${trace_id}-0000000000000073-01" \
    --data-urlencode "query=$(s3_query "'${secret}_qspan'")" >/dev/null 2>&1
# The SQL-defined handler: `v` is a receive parameter, `param_w` an undeclared one.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/${handler}?v=${secret}_hrecv&param_w=${secret}_hprm&query_id=${secret}_id_e" >/dev/null 2>&1

# A non-sensitive parameter must NOT be redacted. Issued last on purpose (see the loop below).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${secret}_visible" -d "SELECT 1 FORMAT Null" >/dev/null 2>&1

# The "Request URI" trace entry is written asynchronously, after the HTTP response is sent
# (https://github.com/ClickHouse/ClickHouse/issues/84364), so a single FLUSH LOGS races the
# log write. Retry FLUSH until the positive-control rows have landed: the masked
# param_secret_key line, the Request URI lines of the query_id requests and the non-sensitive
# query_id line (issued last, so all earlier Request URI lines are flushed by then too).
# enable_parallel_replicas=0 keeps these system.text_log reads local (CI randomization may
# otherwise route them to a cluster).
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} --query="SYSTEM FLUSH LOGS text_log"
    landed=$(${CLICKHOUSE_CLIENT} --query="
        SELECT
            countIf(message LIKE '%Request URI%' AND message LIKE '%param_secret_key=[HIDDEN]%') > 0
            AND countIf(message LIKE '%Request URI%' AND message LIKE '%query_id=${secret}\\_id\\_%') >= 5
            AND countIf(message LIKE '%Request URI%' AND message LIKE '%query_id=${secret}\\_visible%') > 0
        FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
        SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0")
    [ "$landed" = "1" ] && break
    sleep 0.5
done

echo "--- secret values must NOT appear in text_log ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
      AND message LIKE '%${secret}\\_skey%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
      AND (message LIKE '%${secret}\\_pw%' OR message LIKE '%${secret}\\_sig%')
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

echo "--- a percent-encoded sensitive name (pass%77ord -> password) must also be redacted ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
      AND message LIKE '%${secret}\\_enc%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

echo "--- the Request URI line for the secret param must be logged with [HIDDEN] ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
      AND message LIKE '%Request URI%'
      AND message LIKE '%param_secret_key=[HIDDEN]%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

echo "--- a non-sensitive parameter value is still visible ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
      AND message LIKE '%Request URI%'
      AND message LIKE '%query_id=${secret}\\_visible%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

echo "--- the query text and bound query parameters must NOT appear in text_log ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT
        countIf(message LIKE '%${secret}\\_qtext%'),
        countIf(message LIKE '%${secret}\\_qname%'),
        countIf(message LIKE '%${secret}\\_qbound%'),
        countIf(message LIKE '%${secret}\\_qspan%')
    FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

echo "--- their Request URI lines are logged with [HIDDEN] ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT
        countIf(message LIKE '%query_id=${secret}\\_id\\_a%' AND message LIKE '%&query=[HIDDEN]%') > 0,
        countIf(message LIKE '%query_id=${secret}\\_id\\_b%' AND message LIKE '%&quer\\%79=[HIDDEN]%') > 0,
        countIf(message LIKE '%query_id=${secret}\\_id\\_c%' AND message LIKE '%&param\\_s=[HIDDEN]&query=[HIDDEN]%') > 0,
        countIf(message LIKE '%query_id=${secret}\\_id\\_d%' AND message LIKE '%&query=[HIDDEN]%') > 0
    FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
      AND message LIKE '%Request URI%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

echo "--- the credential reached executeQuery, which masks it ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
      AND query_id = '${secret}_id_a' AND logger_name = 'executeQuery'
      AND message LIKE '%''AKIAEXAMPLE'', ''[HIDDEN]''%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

echo "--- the clickhouse.uri span attribute hides the query text ---"
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} --query="SYSTEM FLUSH LOGS opentelemetry_span_log"
    uri_hidden=$(${CLICKHOUSE_CLIENT} --query="
        SELECT attribute['clickhouse.uri'] LIKE '%&query=[HIDDEN]%' AND attribute['clickhouse.uri'] NOT LIKE '%${secret}\\_qspan%'
        FROM system.opentelemetry_span_log
        WHERE finish_date >= yesterday() AND operation_name = 'HTTPHandler'
          AND trace_id = UUIDNumToString(toFixedString(unhex('${trace_id}'), 16))
        SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0")
    [ -n "$uri_hidden" ] && break
    sleep 0.5
done
echo "$uri_hidden"

echo "--- the SQL-defined handler hides its receive parameter and param_* in the Request URI line ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT
        countIf(message LIKE '%${secret}\\_hrecv%' OR message LIKE '%${secret}\\_hprm%'),
        countIf(message LIKE '%query_id=${secret}\\_id\\_e%' AND message LIKE '%?v=[HIDDEN]&param\\_w=[HIDDEN]&%') > 0
    FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= toDateTime(${test_start})
      AND message LIKE '%Request URI%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"
