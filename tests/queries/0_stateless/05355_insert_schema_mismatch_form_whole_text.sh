#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `FormRowInputFormat::readField` percent-decodes the value and hands it to `deserializeWholeText` of
# the destination type, so a value inferred as `String` (here, because best-effort inference is
# disabled) may be valid for a `Bool` or a nested destination: `a=%5B1%2C2%5D` is read into an
# `Array(UInt8)` column and `b=true` into a `Bool` one. When a sibling field causes the real parse
# error, the schema-mismatch diagnostic must not blame those columns.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

run() {
    printf '%s' "$1" | $CLICKHOUSE_LOCAL --input_format_tsv_use_best_effort_in_schema_inference=0 \
        --query "CREATE TABLE t ($2) ENGINE = Memory; INSERT INTO t FORMAT Form" 2>&1 | check
}

echo "-- Form: a string value for a Bool column is not a structure mismatch"
run 'b=true&m=x' 'b Bool, m DateTime'

echo "-- Form: a string value for an Array column is not a structure mismatch"
run 'a=%5B1%2C2%5D&m=x' 'a Array(UInt8), m DateTime'

echo "-- Form: text for a numeric column is still a genuine structure mismatch"
run 'b=true&m=x' 'b Bool, m UInt8'
