#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `FormRowInputFormat::readField` percent-decodes the value and hands it to `deserializeWholeText`, so
# a `Bool` column is read by `SerializationBool` exactly as in the other flat-text formats: only the
# configured representations and the fixed literal forms are accepted, and a numeric value such as `2`
# is a genuine structure mismatch the schema-mismatch diagnostic must report. The literal `1` must not
# produce a false positive.
#
# In both cases the second column holds a fractional value for a `UInt8` column, which produces the
# genuine parse error that triggers the diagnostic.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- Form: a numeric value other than 0/1 for a Bool column is a genuine structure mismatch"
printf 'b=2&n=1.5' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Form" 2>&1 | check

echo "-- Form: the numeric literal 1 for a Bool column is valid (no false positive)"
printf 'b=1&n=1.5' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Form" 2>&1 | check
