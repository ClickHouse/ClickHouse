#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CustomSeparatedFormatReader::readField` and `RegexpRowInputFormat::readField` route every field
# through `deserializeFieldByEscapingRule` with the configured escaping rule, so the value-form
# capabilities of the schema-mismatch diagnostic must follow that rule: with `Quoted` a bare number
# for a `String` column is a genuine structure mismatch (`deserializeTextQuoted` requires an opening
# quote, exactly as in `MySQLDump`), and with `JSON` the `input_format_json_read_*_as_strings`
# settings decide whether a typed token is accepted into a `String` column. A quoted value must not
# produce a false positive.
#
# In every case the second column holds a fractional value for a `UInt8` column, which produces the
# genuine parse error that triggers the diagnostic.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- CustomSeparated, Quoted rule: a bare number for a String column is a genuine structure mismatch"
printf "1|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=Quoted --format_custom_field_delimiter='|' \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check

echo "-- CustomSeparated, Quoted rule: a quoted value for a String column is valid (no false positive)"
printf "'x'|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=Quoted --format_custom_field_delimiter='|' \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check

echo "-- CustomSeparated, Quoted rule: a quoted number for a String column is valid (no false positive)"
printf "'1'|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=Quoted --format_custom_field_delimiter='|' \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check

echo "-- Regexp, Quoted rule: a bare number for a String column is a genuine structure mismatch"
printf "1|1.5\n" | $CLICKHOUSE_LOCAL --format_regexp='(.+)\|(.+)' --format_regexp_escaping_rule=Quoted \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Regexp" 2>&1 | check

echo "-- Regexp, Quoted rule: a quoted value for a String column is valid (no false positive)"
printf "'x'|1.5\n" | $CLICKHOUSE_LOCAL --format_regexp='(.+)\|(.+)' --format_regexp_escaping_rule=Quoted \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Regexp" 2>&1 | check

echo "-- CustomSeparated, JSON rule: an array token for a String column is a genuine structure mismatch when input_format_json_read_arrays_as_strings = 0"
printf "[1,2]|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=JSON --format_custom_field_delimiter='|' \
    --input_format_json_read_arrays_as_strings=0 \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check

echo "-- CustomSeparated, JSON rule: an array token for a String column is valid when input_format_json_read_arrays_as_strings = 1 (no false positive)"
printf "[1,2]|1.5\n" | $CLICKHOUSE_LOCAL --format_custom_escaping_rule=JSON --format_custom_field_delimiter='|' \
    --input_format_json_read_arrays_as_strings=1 \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CustomSeparated" 2>&1 | check
