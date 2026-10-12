#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `TemplateRowInputFormat::deserializeField` picks an escaping rule per placeholder, so the
# schema-mismatch diagnostic can derive the value-form capabilities from the rule only when every
# placeholder uses the same one. With a homogeneous `Quoted` row format a bare number for a `String`
# column is a genuine structure mismatch (`deserializeTextQuoted` requires an opening quote), and
# with a homogeneous `JSON` row format the `input_format_json_read_*_as_strings` settings decide.
# For a row format mixing different rules the diagnostic must fall back to the conservative
# defaults: it may miss a mismatch only an escaping rule could reveal, but must not invent one.
#
# In every case the second column holds a fractional value for a `UInt8` column, which produces the
# genuine parse error that triggers the diagnostic.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

ROW_QUOTED="$CLICKHOUSE_TMP/04843_row_quoted.fmt"
ROW_MIXED="$CLICKHOUSE_TMP/04843_row_mixed.fmt"
ROW_JSON="$CLICKHOUSE_TMP/04843_row_json.fmt"
printf '${s:Quoted}|${n:Quoted}' > "$ROW_QUOTED"
printf '${s:Quoted}|${n:Escaped}' > "$ROW_MIXED"
printf '${s:JSON}|${n:JSON}' > "$ROW_JSON"

echo "-- Template, homogeneous Quoted rules: a bare number for a String column is a genuine structure mismatch"
printf "1|1.5\n" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_QUOTED" \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 | check

echo "-- Template, homogeneous Quoted rules: a quoted value for a String column is valid (no false positive)"
printf "'x'|1.5\n" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_QUOTED" \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 | check

echo "-- Template, mixed Quoted and Escaped rules: no single value-form contract, the diagnostic stays conservative"
printf "1|1.5\n" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_MIXED" \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 | check

echo "-- Template, homogeneous JSON rules: a number token for a String column is a genuine structure mismatch when input_format_json_read_numbers_as_strings = 0"
printf "1|1.5\n" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_JSON" --input_format_json_read_numbers_as_strings=0 \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 | check

echo "-- Template, homogeneous JSON rules: a number token for a String column is valid when input_format_json_read_numbers_as_strings = 1 (no false positive)"
printf "1|1.5\n" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_JSON" --input_format_json_read_numbers_as_strings=1 \
    --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 | check

rm -f "$ROW_QUOTED" "$ROW_MIXED" "$ROW_JSON"
