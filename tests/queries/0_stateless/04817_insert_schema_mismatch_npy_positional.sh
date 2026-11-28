#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# `Npy` supports reading a subset of columns, but its parser writes the single column positionally,
# while its schema reader always names that column `array`. The schema-mismatch diagnostic must
# compare `Npy` positionally: matching it by name would either report a bogus `array` vs `<column>`
# structure mismatch on a plain data-corruption error (with `input_format_skip_unknown_fields` = 0),
# or silently drop a genuine type mismatch by treating the `array` column as unknown and skipping it
# (with the default `input_format_skip_unknown_fields` = 1).
# The whole flow runs inside clickhouse-local: the `INSERT ... FROM INFILE` diagnostic path of
# `ClientBase` is shared between clickhouse-client and clickhouse-local.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_INT="${CLICKHOUSE_TMP}/04817_int_${CLICKHOUSE_DATABASE}.npy"
DATA_INT_TRUNCATED="${CLICKHOUSE_TMP}/04817_int_truncated_${CLICKHOUSE_DATABASE}.npy"
DATA_STR_TRUNCATED="${CLICKHOUSE_TMP}/04817_str_truncated_${CLICKHOUSE_DATABASE}.npy"

$CLICKHOUSE_LOCAL -q "SELECT number::Int64 AS x FROM numbers(3) FORMAT Npy" > "$DATA_INT"
head -c $(( $(stat -c%s "$DATA_INT") - 4 )) "$DATA_INT" > "$DATA_INT_TRUNCATED"
$CLICKHOUSE_LOCAL -q "SELECT 'abc' AS x FROM numbers(3) FORMAT Npy" | head -c -2 > "$DATA_STR_TRUNCATED"

echo "-- truncated Npy of Int64 into a matching Int64 column: a plain corruption error, no structure mismatch (even with skip_unknown_fields = 0)"
$CLICKHOUSE_LOCAL --input_format_skip_unknown_fields 0 -q "
    CREATE TABLE t (x Int64) ENGINE = Memory;
    INSERT INTO t FROM INFILE '$DATA_INT_TRUNCATED' FORMAT Npy;
" 2>&1 | check

echo "-- truncated Npy of String into an Int64 column: the genuine type mismatch is explained positionally"
$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t (x Int64) ENGINE = Memory;
    INSERT INTO t FROM INFILE '$DATA_STR_TRUNCATED' FORMAT Npy;
" 2>&1 | check

rm -f "$DATA_INT" "$DATA_INT_TRUNCATED" "$DATA_STR_TRUNCATED"
