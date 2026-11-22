#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An RFC 2047 encoded word must not decode into CR, LF or NUL inside a header value:
# such a value could split the header when it is forwarded (e.g. to an HTTP authentication server).

function check_rejected()
{
    local header="$1"
    ${CLICKHOUSE_CURL} -sS -H "$header" -w '%{http_code}\n' "${CLICKHOUSE_URL}&query=SELECT+1" | grep -o -F -e 'contains CR, LF or NUL' -e '400'
}

echo "-- Q-encoded CRLF"
check_rejected 'X-Test: =?UTF-8?Q?x=0D=0AInjected:_1?='
echo "-- Q-encoded LF"
check_rejected 'X-Test: =?UTF-8?Q?x=0AInjected:_1?='
echo "-- B-encoded CRLF"
check_rejected 'X-Test: =?UTF-8?B?eA0KSW5qZWN0ZWQ6IDE=?='
echo "-- Q-encoded NUL"
check_rejected 'X-Test: =?UTF-8?Q?x=00y?='
echo "-- Encoded word in the middle of the value"
check_rejected 'X-Test: prefix =?UTF-8?Q?=0D=0A?= suffix'

echo "-- Other encoded words are still decoded"
${CLICKHOUSE_CURL} -sS -H 'X-Test: =?UTF-8?Q?caf=C3=A9_au_lait?=' "${CLICKHOUSE_URL}&allow_get_client_http_header=1" -d "SELECT getClientHTTPHeader('X-Test')"
${CLICKHOUSE_CURL} -sS -H 'X-Test: =?UTF-8?B?Y2Fmw6k=?=' "${CLICKHOUSE_URL}&allow_get_client_http_header=1" -d "SELECT getClientHTTPHeader('X-Test')"
