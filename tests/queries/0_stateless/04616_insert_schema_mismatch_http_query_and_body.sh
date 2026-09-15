#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The HTTP interface allows the INSERT text to be split between the `query` URL parameter and the request
# body: an inline data prefix in `query` (so `ASTInsertQuery::data` is non-null) followed by the rest of
# the rows in the body (parked in `ASTInsertQuery::tail`). The parse-error diagnostic must sample the
# whole streamed data, not just the inline prefix, otherwise a mismatch that only appears in the body row
# is not explained. Here the inline prefix is a valid all-integer row and the body row has strings in the
# integer columns.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_query_and_body (c1 Int64, c2 Int64, c3 Int64) ENGINE = Memory"

echo "-- HTTP interface, inline prefix in the query parameter and the failing row in the body, type mismatch"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0&query=INSERT%20INTO%20test_query_and_body%20FORMAT%20TSV%0A1%092%093" \
    --data-binary $'4\tpage_view\t/users/profile\n' 2>&1 | check

echo "-- HTTP interface, inline prefix in the query parameter and a matching row in the body (no false positive)"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0&query=INSERT%20INTO%20test_query_and_body%20FORMAT%20TSV%0A1%092%093" \
    --data-binary $'4\t5\t6\n' 2>&1 | check

$CLICKHOUSE_CLIENT -q "DROP TABLE test_query_and_body"
