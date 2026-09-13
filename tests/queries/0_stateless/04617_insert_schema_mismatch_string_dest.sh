#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The schema-mismatch diagnostic must not attach a misleading "structure mismatch" explanation when a
# value that schema inference widened to a richer scalar type (a number, a boolean, ...) is inserted into
# a `String` column. The text parsers accept such a value into a `String` destination: `TSV` / `CSV` read
# the raw field verbatim, and `JSONEachRow` reads a JSON number / boolean into a `String` under the default
# `input_format_json_read_numbers_as_strings` / `read_bools_as_strings` settings. Here the parse fails only
# because a fractional value cannot be read into an integer column, not because the structures disagree.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- TSV, a numeric-looking value into a String column plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\t1.5\n2\t2.5\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, a number and a boolean into String columns plus an unrelated bad numeric column (no false positive)"
printf 'CREATE TABLE t (s String, b String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"s": 1, "b": true, "n": 1.5}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- TSV, genuinely non-numeric text into a numeric column is still explained (flagship signal preserved)"
printf 'CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\nhello\tpage_view\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
