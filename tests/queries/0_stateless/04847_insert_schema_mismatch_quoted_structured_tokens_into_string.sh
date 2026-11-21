#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In the quoted-text formats (`MySQLDump`, and `CustomSeparated` / `Regexp` / `Template` with the
# `Quoted` escaping rule) a `String` column accepts a value only when the token was actually quoted:
# an unquoted bracket or word token — the only thing the `Quoted` rule's schema inference can have
# derived an `Array` / `Tuple` / `Map` / `Bool` from — is rejected by
# `SerializationString::deserializeTextQuoted`, which requires an opening quote, so such an inferred
# type going into a `String` column is a genuine structure mismatch. A quoted token — from which
# inference derives a `String` or a date — must not produce a false positive.
#
# Where the mismatch is on the structured token itself, that token alone already produces the parse
# error; in the control cases the second column holds a fractional value for a `UInt8` column, which
# produces the genuine parse error that triggers the diagnostic.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- CustomSeparated, Quoted rule: an array token for a String column is a genuine structure mismatch"
printf "[1,2]|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=Quoted --format_custom_field_delimiter='|' \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check

echo "-- CustomSeparated, Quoted rule: a tuple token for a String column is a genuine structure mismatch"
printf "(1,2)|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=Quoted --format_custom_field_delimiter='|' \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check

echo "-- CustomSeparated, Quoted rule: a map token for a String column is a genuine structure mismatch"
printf "{1:2}|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=Quoted --format_custom_field_delimiter='|' \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check

echo "-- CustomSeparated, Quoted rule: a bool token for a String column is a genuine structure mismatch"
printf "true|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=Quoted --format_custom_field_delimiter='|' \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check

echo "-- CustomSeparated, Quoted rule: a quoted date for a String column is valid (no false positive)"
printf "'2020-01-01'|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=Quoted --format_custom_field_delimiter='|' \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check

echo "-- CustomSeparated, Escaped rule: an array-shaped raw field for a String column is valid (no false positive)"
printf "[1,2]|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=Escaped --format_custom_field_delimiter='|' \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check

echo "-- Regexp, Quoted rule: an array token for a String column is a genuine structure mismatch"
printf "[1,2]|1.5\n" | $CLICKHOUSE_LOCAL --format_regexp='(.+)\|(.+)' --format_regexp_escaping_rule=Quoted \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Regexp" 2>&1 | check

echo "-- MySQLDump: an array token for a String column is a genuine structure mismatch"
printf "INSERT INTO t VALUES ([1,2], 1.5);\n" | $CLICKHOUSE_LOCAL \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- MySQLDump: a quoted date for a String column is valid (no false positive)"
printf "INSERT INTO t VALUES ('2020-01-01', 1.5);\n" | $CLICKHOUSE_LOCAL \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check
