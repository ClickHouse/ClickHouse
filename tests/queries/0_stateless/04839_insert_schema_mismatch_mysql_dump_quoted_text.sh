#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `MySQLDumpRowInputFormat::readField` reads every value with `deserializeTextQuoted`, so, unlike the
# other flat-text formats, the on-wire form of a value has to match what the destination accepts. A bare
# number — the only thing schema inference can have derived a numeric type from — is rejected by every
# destination whose quoted-text deserializer requires an opening quote (`String`, `FixedString`, `UUID`,
# `IPv4`, `IPv6`, `Date` / `Date32`, `Enum`) and by a `Bool` column unless it is `1` / `0`, so all of
# those are genuine structure mismatches the schema-mismatch diagnostic must report.
#
# It must stay silent where the value really is accepted: `DateTime` reads a bare number as a Unix
# timestamp, `Decimal` reads the number itself, and a quoted value — from which inference derives a
# `String` or a date — is read into a `String` column just as in the other flat-text formats.
#
# In every case the last column holds a fractional value for a `UInt8` column, which produces the
# genuine parse error that triggers the diagnostic.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- MySQLDump: an unquoted value for a String column is a genuine structure mismatch"
printf 'INSERT INTO t VALUES (1, 1.5);\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- MySQLDump: a numeric value other than 0/1 for a Bool column is a genuine structure mismatch"
printf 'INSERT INTO t VALUES (2, 1.5);\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- MySQLDump: an unquoted value for a Date column is a genuine structure mismatch"
printf 'INSERT INTO t VALUES (1, 1.5);\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (d Date, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- MySQLDump: an unquoted value for an Enum column is a genuine structure mismatch"
printf 'INSERT INTO t VALUES (1, 1.5);\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (e Enum8('a' = 1, 'b' = 2), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- MySQLDump: the numeric literal 1 for a Bool column is valid (no false positive)"
printf 'INSERT INTO t VALUES (1, 1.5);\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (b Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- MySQLDump: a bare number for a DateTime column is a Unix timestamp (no false positive)"
printf 'INSERT INTO t VALUES (1, 1.5);\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (d DateTime, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- MySQLDump: a bare number for a Decimal column is valid (no false positive)"
printf 'INSERT INTO t VALUES (1, 1.5);\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (d Decimal(9, 2), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- MySQLDump: a quoted date for a String column is read verbatim (no false positive)"
printf "INSERT INTO t VALUES ('2020-01-01', 1.5);\n" | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- MySQLDump: a quoted string for a String column is valid (no false positive)"
printf "INSERT INTO t VALUES ('abc', 1.5);\n" | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check
