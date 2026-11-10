#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `INSERT ... SELECT` takes its data from the SELECT and never reads the HTTP request body (`executeQuery`
# drops the request tail for it), so a handler running it must not get the body-reading contract: a lengthless
# bodyless PUT / DELETE must be answered normally instead of with `411 Length Required`, and read-only methods
# may be mixed into its METHODS clause. The exception is `INSERT ... SELECT ... FROM input(...)`, which is fed
# from the request body and therefore keeps the contract.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
P="/hisb_${DB}"
HSEL="hisb_sel_${DB}"
HMIX="hisb_mix_${DB}"
HINPUT="hisb_input_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$HSEL\`; DROP HANDLER IF EXISTS \`$HMIX\`; DROP HANDLER IF EXISTS \`$HINPUT\`;"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t (x UInt64) ENGINE = MergeTree ORDER BY x"

# A lengthless non-chunked body ends only when the connection closes, so a server that waits for it deadlocks
# against a client that waits for the response. Bound these requests well below the test timeout, so that such
# a regression shows up as a diff here instead of killing the whole test run.
# Keep them silent and turn a nonzero exit into a stdout line: the harness fails on any stderr before it
# compares the reference, so an error line there would take the place of that diff.
CURL_BOUNDED="${CLICKHOUSE_CURL_COMMAND} -q -s --max-time 30"

echo "=== an INSERT ... SELECT handler may mix read-only and body-carrying methods ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HMIX\` URL '${P}/mix' METHODS (GET, PUT, DELETE) AS INSERT INTO ${DB}.t SELECT 1"
$CLICKHOUSE_CLIENT -q "SELECT methods FROM system.handlers WHERE name = '$HMIX'"

echo "=== but it still needs at least one mutating method ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HSEL\` URL '${P}/sel' METHODS (GET) AS INSERT INTO ${DB}.t SELECT 2" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== a lengthless bodyless PUT to an INSERT ... SELECT handler works ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HSEL\` URL '${P}/sel' METHODS (PUT, DELETE) AS INSERT INTO ${DB}.t SELECT 3"
${CURL_BOUNDED} -o /dev/null -w '%{http_code}\n' -X PUT "${BASE}${P}/sel" || echo "curl failed: $?"

echo "=== a lengthless bodyless DELETE to an INSERT ... SELECT handler works ==="
${CURL_BOUNDED} -o /dev/null -w '%{http_code}\n' -X DELETE "${BASE}${P}/sel" || echo "curl failed: $?"

echo "=== both requests inserted their row ==="
$CLICKHOUSE_CLIENT -q "SELECT count(), min(x), max(x) FROM ${DB}.t"

echo "=== an INSERT ... SELECT ... FROM input(...) handler does read the body: read-only methods are rejected ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HINPUT\` URL '${P}/input' METHODS (GET, PUT) AS INSERT INTO ${DB}.t SELECT x FROM input('x UInt64') FORMAT TSV" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

echo "=== and a lengthless request to it is answered with 411 ==="
$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$HINPUT\` URL '${P}/input' METHODS (PUT) AS INSERT INTO ${DB}.t SELECT x FROM input('x UInt64') FORMAT TSV"
${CLICKHOUSE_CURL} -sS -X PUT -I "${BASE}${P}/input" | grep -c '411 Length Required'

echo "=== while a request with a length inserts the body ==="
${CLICKHOUSE_CURL} -sS -X PUT -H 'Content-Type: text/plain' --data-binary '100
200' "${BASE}${P}/input"
$CLICKHOUSE_CLIENT -q "SELECT count(), min(x), max(x) FROM ${DB}.t"
