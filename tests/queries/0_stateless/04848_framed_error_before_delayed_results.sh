#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `http_push_delayed_results_throw` fail point, which affects the whole
# server. It fires on the next buffered HTTP response anywhere on the server, so a concurrent HTTP
# query from another test could consume the injected fault - making this test miss its own error and
# the other test throw spuriously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `http_wait_end_of_query` the framed success stream is buffered in a cascade that does not
# touch the real response until `pushDelayedResults`. A failure before a single delayed byte
# reaches the response (finalizing the cascade, reopening a temporary file to re-read it) leaves
# the response completely untouched: no header sent, nothing buffered in the response stream. The
# fail-close rule for started framed transmissions (see `04693`) must not fire here - the client
# must get a proper HTTP error response, not an aborted connection.

echo '--- buffered: a failure before any delayed byte is pushed yields a proper error response'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT http_push_delayed_results_throw"
HTTP_CODE_FILE=$(mktemp)
RESPONSE=$(${CLICKHOUSE_CURL} -s -w '%{http_code}' -o "${HTTP_CODE_FILE}.body" \
    "${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString&http_wait_end_of_query=1" \
    -d "SELECT number AS x FROM numbers(3) FORMAT JSONEachRow")
CURL_EXIT=$?
BODY=$(cat "${HTTP_CODE_FILE}.body")
rm -f "${HTTP_CODE_FILE}" "${HTTP_CODE_FILE}.body"
[[ $CURL_EXIT -eq 0 ]] && echo 'the connection was not aborted: OK' \
    || echo "MISMATCH: curl exit code $CURL_EXIT: the connection was aborted"
[[ "$RESPONSE" != "200" ]] && echo 'a non-200 status was sent: OK' \
    || echo 'MISMATCH: the failure was reported with HTTP 200'
[[ "$BODY" == *'FAULT_INJECTED'* ]] && echo 'the error reason reaches the client: OK' \
    || echo "MISMATCH: no error message in the response body: $BODY"
[[ "$BODY" != *'"packet":"data"'* ]] && echo 'no partial packet stream before the error: OK' \
    || echo 'MISMATCH: buffered data packets leaked into the error response'
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT http_push_delayed_results_throw"
