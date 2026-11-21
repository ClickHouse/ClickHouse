#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# When an async insert exceeds `async_insert_max_data_size`, the async insert queue returns
# `TOO_MUCH_DATA` and the query falls back to a synchronous insert. On that fallback `executeQuery`
# moves the payload out of `ASTInsertQuery::data` (nulling it) into the streamed `tail`, so the
# parse-error diagnostic can no longer re-read the inline data. It must still explain a structure
# mismatch by capturing a bounded prefix of the streamed bytes. Reproduced over HTTP with a tiny
# `async_insert_max_data_size`, so any non-empty payload triggers the synchronous fallback.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_toomuch (c1 Int64, c2 Int64, c3 Int64) ENGINE = Memory"

echo "-- HTTP interface, async insert falling back to synchronous (TOO_MUCH_DATA), type mismatch"
printf 'INSERT INTO test_toomuch FORMAT TSV\n1\tpage_view\t/users/profile\n' \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&async_insert_max_data_size=1" --data-binary @- 2>&1 | check

echo "-- HTTP interface, async insert falling back to synchronous (TOO_MUCH_DATA), matching data (no false positive)"
printf 'INSERT INTO test_toomuch FORMAT TSV\n1\t2\t3\n' \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&async_insert_max_data_size=1" --data-binary @- 2>&1 | check

$CLICKHOUSE_CLIENT -q "DROP TABLE test_toomuch"
