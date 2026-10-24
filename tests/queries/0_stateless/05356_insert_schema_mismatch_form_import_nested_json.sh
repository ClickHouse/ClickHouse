#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `input_format_import_nested_json` maps a top-level JSON object onto the dotted columns of a `Nested`
# destination only in the `JSONEachRowRowInputFormat` family. `FormRowInputFormat::readField` looks
# field names up exactly regardless of that setting, so for `Form` the schema-mismatch diagnostic must
# still explain a genuine mismatch when the setting is enabled and the destination has `Nested` columns.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

run() {
    printf '%s' "$1" | $CLICKHOUSE_LOCAL --input_format_import_nested_json="$2" \
        --query "CREATE TABLE t (n Nested(i UInt8), x UInt8) ENGINE = Memory; INSERT INTO t FORMAT Form" 2>&1 | check
}

echo "-- Form: a mismatch is explained with input_format_import_nested_json disabled"
run 'n=%7B%22i%22%3A%5B1%5D%7D&x=1.5' 0

echo "-- Form: a mismatch is explained with input_format_import_nested_json enabled"
run 'n=%7B%22i%22%3A%5B1%5D%7D&x=1.5' 1
