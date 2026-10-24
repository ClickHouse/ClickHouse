#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# A bare number into a `FixedString` column is a genuine structure mismatch in the typed-token JSON
# formats — `SerializationFixedString::deserializeTextJSON` requires a quoted string and rejects a number
# regardless of `input_format_json_read_numbers_as_strings` (that setting covers only the plain `String`
# destination) — so the schema-mismatch diagnostic must explain it there. The flat-text formats (`TSV`,
# `CSV`) read the raw field verbatim into a `FixedString` column, so a number is accepted and an
# unrelated parse error elsewhere in the row must not pick up a misleading explanation.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONEachRow, number into FixedString (genuine mismatch)"
printf 'CREATE TABLE t (f FixedString(3)) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"f": 1}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, number into FixedString with input_format_json_read_numbers_as_strings = 1 (still a genuine mismatch: the setting covers only the String destination)"
printf 'CREATE TABLE t (f FixedString(3)) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"f": 1}\n' \
    | $CLICKHOUSE_LOCAL --input_format_json_read_numbers_as_strings 1 2>&1 | check

echo "-- TSV, number into FixedString plus an unrelated bad numeric column (read verbatim, no false positive)"
printf 'CREATE TABLE t (f FixedString(3), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\t1.5\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
