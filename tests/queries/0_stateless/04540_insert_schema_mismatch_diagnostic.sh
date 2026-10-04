#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# When parsing of the data being inserted fails, and the structure inferred from the data does not
# match the structure the query expects, the error message should explain the mismatch. Here TSV
# data with strings in the 2nd and 3rd columns is inserted into a table of integer columns.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- clickhouse-local, type mismatch"
printf 'CREATE TABLE t (c1 Int64, c2 Int64, c3 Int64) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\tpage_view\t/users/profile\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- clickhouse-local, matching data (no false positive)"
printf 'CREATE TABLE t (c1 Int64, c2 Int64, c3 Int64) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\t2\t3\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_mismatch (c1 Int64, c2 Int64, c3 Int64) ENGINE = Memory"

echo "-- clickhouse-client, synchronous insert, type mismatch"
printf 'INSERT INTO test_mismatch FORMAT TSV\n1\tpage_view\t/users/profile\n' \
    | $CLICKHOUSE_CLIENT --async_insert 0 2>&1 | check

echo "-- clickhouse-client, asynchronous insert, type mismatch"
printf 'INSERT INTO test_mismatch FORMAT TSV\n1\tpage_view\t/users/profile\n' \
    | $CLICKHOUSE_CLIENT --async_insert 1 --wait_for_async_insert 1 2>&1 | check

echo "-- HTTP interface, synchronous insert, type mismatch"
printf 'INSERT INTO test_mismatch FORMAT TSV\n1\tpage_view\t/users/profile\n' \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0" --data-binary @- 2>&1 | check

echo "-- HTTP interface, asynchronous insert, type mismatch"
printf 'INSERT INTO test_mismatch FORMAT TSV\n1\tpage_view\t/users/profile\n' \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1" --data-binary @- 2>&1 | check

$CLICKHOUSE_CLIENT -q "DROP TABLE test_mismatch"
