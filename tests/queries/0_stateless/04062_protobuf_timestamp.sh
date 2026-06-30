#!/usr/bin/env bash
# Tags: no-fasttest
# Uses clickhouse-client (not clickhouse-local): the bundled google protos path is only configured for the server.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

SCHEMA="$SCHEMADIR/04062_protobuf_timestamp:Message"

# Round-trip a column of the given type through a google.protobuf.Timestamp proto field and print the result.
roundtrip() {
    local col_type="$1"
    local values="$2"

    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS tbl_04062;
        DROP TABLE IF EXISTS roundtrip_04062;
        CREATE TABLE tbl_04062 (ts ${col_type}) ENGINE = MergeTree ORDER BY tuple();
        CREATE TABLE roundtrip_04062 (ts ${col_type}) ENGINE = MergeTree ORDER BY tuple();
        INSERT INTO tbl_04062 VALUES ${values};
    "

    local bin
    bin=$(mktemp "$CURDIR/04062_protobuf_timestamp.XXXXXX.binary")
    $CLICKHOUSE_CLIENT --query "SELECT * FROM tbl_04062 ORDER BY ts FORMAT Protobuf SETTINGS format_schema = '$SCHEMA'" > "$bin"
    $CLICKHOUSE_CLIENT --query "INSERT INTO roundtrip_04062 SETTINGS format_schema = '$SCHEMA' FORMAT Protobuf" < "$bin"
    rm "$bin"

    $CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip_04062 ORDER BY ts"
}

# Feed a google.protobuf.Timestamp whose seconds fall outside the target column's range and show the rejection.
expect_out_of_range() {
    local col_type="$1"
    local seconds="$2"

    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS overflow_04062;
        CREATE TABLE overflow_04062 (ts ${col_type}) ENGINE = MergeTree ORDER BY tuple();
    "

    local bin
    bin=$(mktemp "$CURDIR/04062_protobuf_timestamp.XXXXXX.binary")
    $CLICKHOUSE_CLIENT --query "SELECT toDateTime64(${seconds}, 0, 'UTC') AS ts FORMAT Protobuf SETTINGS format_schema = '$SCHEMA'" > "$bin"
    $CLICKHOUSE_CLIENT --query "INSERT INTO overflow_04062 SETTINGS format_schema = '$SCHEMA' FORMAT Protobuf" < "$bin" 2>&1 | grep -o "Could not convert value.*column 'ts'" ||:
    rm "$bin"
}

# Feed a hand-crafted google.protobuf.Timestamp and show that it is rejected, not silently coerced.
expect_rejected() {
    local col_type="$1"
    local message_bytes="$2"

    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS rejected_04062;
        CREATE TABLE rejected_04062 (ts ${col_type}) ENGINE = MergeTree ORDER BY tuple();
    "

    local bin
    bin=$(mktemp "$CURDIR/04062_protobuf_timestamp.XXXXXX.binary")
    printf "$message_bytes" > "$bin"
    $CLICKHOUSE_CLIENT --query "INSERT INTO rejected_04062 SETTINGS format_schema = '$SCHEMA' FORMAT Protobuf" < "$bin" 2>&1 | grep -o "Could not convert value.*column 'ts'" ||:
    rm "$bin"
}

echo "DateTime64(9):"
roundtrip "DateTime64(9, 'UTC')" "('2022-01-22 12:34:56.789012345'), ('1969-12-31 23:59:59.5'), ('1970-01-01 00:00:00')"

echo "DateTime64(3):"
roundtrip "DateTime64(3, 'UTC')" "('2022-01-22 12:34:56.789')"

echo "DateTime:"
roundtrip "DateTime('UTC')" "('2022-01-22 12:34:56')"

# Under Nullable the epoch (seconds=0, nanos=0) is a non-null value, not NULL: its empty Timestamp message must survive the round-trip.
echo "Nullable(DateTime64):"
roundtrip "Nullable(DateTime64(9, 'UTC'))" "(NULL), ('1970-01-01 00:00:00')"

echo "Nullable(DateTime):"
roundtrip "Nullable(DateTime('UTC'))" "(NULL), ('1970-01-01 00:00:00')"

# The maximum valid nanos (999999999) must still be accepted at the boundary.
echo "Max nanos (DateTime64):"
roundtrip "DateTime64(9, 'UTC')" "('1970-01-01 00:00:00.999999999')"

# A Timestamp whose seconds fall outside the target column's range must be rejected, not silently wrapped.
echo "Out of range (DateTime64):"
expect_out_of_range "DateTime64(9, 'UTC')" "10000000000"

echo "Out of range (DateTime):"
expect_out_of_range "DateTime('UTC')" "4294967296"

# A Timestamp whose nanos fall outside [0, 999999999] must be rejected, not normalized into another value.
echo "Invalid nanos = 1000000000 (DateTime64):"
expect_rejected "DateTime64(9, 'UTC')" '\x08\x0A\x06\x10\x80\x94\xEB\xDC\x03' # Timestamp{nanos=1000000000}

echo "Invalid nanos = -1 (DateTime):"
expect_rejected "DateTime('UTC')" '\x0D\x0A\x0B\x10\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x01' # Timestamp{nanos=-1}

# Seconds outside the DateTime64 range [1900-01-01, 2299-12-31] must be rejected, not stored as a wrapping value.
echo "Seconds before 1900 (DateTime64):"
expect_rejected "DateTime64(9, 'UTC')" '\x08\x80\xFF\xF6\x81\xEC\xFF\xFF\xFF\xFF\x01' # Timestamp{seconds=-5364662400} = 1800-01-01

echo "Seconds after 2299 (DateTime64):"
expect_rejected "DateTime64(0, 'UTC')" '\x08\x80\xB6\xD7\xE5\x26' # Timestamp{seconds=10413792000} = 2300-01-01
