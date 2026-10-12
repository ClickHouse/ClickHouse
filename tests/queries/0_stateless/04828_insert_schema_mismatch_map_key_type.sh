#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A JSON object going into a `Map` destination: the object keys are parsed by the `Map` key type
# (`SerializationMap::deserializeTextJSONImpl` reads every key with the key serialization), so a key
# the key type cannot parse (e.g. `"x"` for a `Map(UInt64, ...)`) is a genuine structure mismatch
# the schema-mismatch diagnostic must report, while keys the key type does parse (e.g. `"42"` for
# `Map(UInt64, ...)` or any key for `Map(String, ...)`) must not produce a false positive.
#
# In the no-false-positive cases the field `n` holds a fractional value for a `UInt8` column, which
# produces the genuine parse error that triggers the diagnostic.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- a non-numeric object key for a Map(UInt64, ...) column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (m Map(UInt64, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow"
    echo '{"m":{"x":1},"n":1.5}'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- a numeric object key for a Map(UInt64, ...) column is valid (no false positive)"
{
    echo "CREATE TABLE t (m Map(UInt64, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow"
    echo '{"m":{"42":1},"n":1.5}'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- any object key for a Map(String, ...) column is valid (no false positive)"
{
    echo "CREATE TABLE t (m Map(String, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow"
    echo '{"m":{"x":1},"n":1.5}'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- a non-date object key for a Map(Date, ...) column is a genuine structure mismatch"
{
    echo "CREATE TABLE t (m Map(Date, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow"
    echo '{"m":{"x":1},"n":1.5}'
} | $CLICKHOUSE_LOCAL 2>&1 | check
