#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Schema inference keeps a bare `true` / `false` token as `Bool`, and the `UInt8`-backed `Bool` looks
# like a generic numeric type — but the parsers accept the token in far fewer destinations than a
# numeric token. In JSON, `SerializationNumber<T>::deserializeTextJSON` reads it into a numeric
# column only under `input_format_json_read_bools_as_numbers` (`UInt8` / `Int8` accept it always),
# and the `DateTime` / `Decimal` / ... deserializers reject it outright; in the flat-text formats the
# numeric readers parse no word forms at all. The schema-mismatch diagnostic must report those cases
# and must not flag the destinations that do accept the token.
#
# The second column always holds a fractional value for a `UInt8` column, so the rows with a
# compatible first column still produce the genuine parse error that triggers the diagnostic.
# `input_format_json_read_bools_as_numbers` is pinned explicitly in every JSON case.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONEachRow: a bool token for a UInt16 column with input_format_json_read_bools_as_numbers = 0 is a genuine structure mismatch"
echo '{"x": true, "y": 1.5}' | $CLICKHOUSE_LOCAL --input_format_json_read_bools_as_numbers 0 --query "CREATE TABLE t (x UInt16, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check

echo "-- JSONEachRow: a bool token for a UInt16 column with input_format_json_read_bools_as_numbers = 1 is valid (no false positive)"
echo '{"x": true, "y": 1.5}' | $CLICKHOUSE_LOCAL --input_format_json_read_bools_as_numbers 1 --query "CREATE TABLE t (x UInt16, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check

echo "-- JSONEachRow: a bool token for a UInt8 column is always valid, regardless of the setting (no false positive)"
echo '{"x": true, "y": 1.5}' | $CLICKHOUSE_LOCAL --input_format_json_read_bools_as_numbers 0 --query "CREATE TABLE t (x UInt8, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check

echo "-- JSONEachRow: a bool token for a DateTime column is a genuine structure mismatch even with input_format_json_read_bools_as_numbers = 1"
echo '{"x": true, "y": 1.5}' | $CLICKHOUSE_LOCAL --input_format_json_read_bools_as_numbers 1 --query "CREATE TABLE t (x DateTime, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check

echo "-- TSV: the word true for a UInt16 column is a genuine structure mismatch (the flat-text numeric readers parse no word forms)"
printf 'true\t1.5\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (x UInt16, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV" 2>&1 | check

echo "-- TSV: the word true for a Bool column is valid (no false positive)"
printf 'true\t1.5\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (x Bool, y UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV" 2>&1 | check
