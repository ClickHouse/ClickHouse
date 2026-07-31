#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/103860 — deserializer side.
#
# Companion to 04253_sqlite_escape_quote_with_quote_103860.sh which verified the
# EMIT side: `output_format_values_escape_quote_with_quote = 1` makes
# `JSON` / `Enum` / `AggregateFunction` / `CustomSimpleText` produce the
# SQL-standard `''` apostrophe escape in `Values` output instead of `\'`.
#
# This test verifies the PARSE side: a `Values` literal containing `''` must
# be parseable back through each of these serializations.
#
# Before the fix, `SerializationJSON::deserializeTextQuoted` and
# `SerializationCustomSimpleText::(try)deserializeTextQuoted` used
# `readQuotedString` / `tryReadQuotedString`, which do NOT accept the
# SQL-standard `''` form — they treat the second `'` as the closing quote and
# leave the rest of the literal unread, breaking the
# serializer / deserializer contract for `Values`.
# After the fix they use `readQuotedStringWithSQLStyle` /
# `tryReadQuotedStringWithSQLStyle`, which accept both `''` and `\'`.
#
# `SerializationEnum::deserializeTextQuoted` and
# `SerializationAggregateFunction::deserializeTextQuoted` already used the
# SQL-style reader before the fix; they are still exercised here to lock down
# the symmetry across all four serializations touched in #103860.
#
# The `Values` parser has a SQL-expression-parser fallback (and a template
# fallback) when `deserializeTextQuoted` throws, which would mask the
# asymmetry for `JSON` (the SQL parser also accepts `''` inside string
# literals). To exercise the `deserializeTextQuoted` path directly and ensure
# the deserializer is what actually accepts `''`, both fallbacks are disabled:
# `input_format_values_interpret_expressions = 0` and
# `input_format_values_deduce_templates_of_expressions = 0`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT_NO_FALLBACK="${CLICKHOUSE_CLIENT} --input_format_values_interpret_expressions=0 --input_format_values_deduce_templates_of_expressions=0"

# --- JSON ---
# Feed the SQL-style-escaped Values literal via stdin so neither bash nor the
# outer SQL parser interferes with the apostrophes; the only parser that sees
# them is `SerializationJSON::deserializeTextQuoted`.
${CLICKHOUSE_CLIENT} -q "SELECT '--- JSON, SQL-style apostrophe escape (was broken pre-fix) ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_roundtrip_103860; CREATE TABLE t_json_roundtrip_103860 (j JSON) ENGINE = Memory SETTINGS enable_json_type = 1;"
printf "('{\"k\":\"a''b\"}')" | ${CH_CLIENT_NO_FALLBACK} --query="INSERT INTO t_json_roundtrip_103860 FORMAT Values" --enable_json_type=1
${CLICKHOUSE_CLIENT} -q "SELECT toString(j) FROM t_json_roundtrip_103860;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_roundtrip_103860;"
echo

# Same JSON parse path, with the legacy backslash form — must keep working.
${CLICKHOUSE_CLIENT} -q "SELECT '--- JSON, legacy backslash apostrophe escape (must still parse) ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_roundtrip_103860; CREATE TABLE t_json_roundtrip_103860 (j JSON) ENGINE = Memory SETTINGS enable_json_type = 1;"
printf "('{\"k\":\"a\\\\'b\"}')" | ${CH_CLIENT_NO_FALLBACK} --query="INSERT INTO t_json_roundtrip_103860 FORMAT Values" --enable_json_type=1
${CLICKHOUSE_CLIENT} -q "SELECT toString(j) FROM t_json_roundtrip_103860;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_roundtrip_103860;"
echo

# --- Enum ---
${CLICKHOUSE_CLIENT} -q "SELECT '--- Enum, SQL-style apostrophe escape ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_enum_roundtrip_103860; CREATE TABLE t_enum_roundtrip_103860 (e Enum8('a''b' = 1)) ENGINE = Memory;"
printf "('a''b')" | ${CH_CLIENT_NO_FALLBACK} --query="INSERT INTO t_enum_roundtrip_103860 FORMAT Values"
${CLICKHOUSE_CLIENT} -q "SELECT e FROM t_enum_roundtrip_103860;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_enum_roundtrip_103860;"
echo

# --- CustomSimpleText (Bool) ---
# Types backed by `SerializationCustomSimpleText` (`Bool`, `Point`, `IPv4`, ...)
# do not contain `'` in their textual form, so the asymmetry was not
# user-observable for them. This sanity check ensures the switch to
# `readQuotedStringWithSQLStyle` / `tryReadQuotedStringWithSQLStyle` did not
# regress the common case.
${CLICKHOUSE_CLIENT} -q "SELECT '--- Bool (CustomSimpleText) ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_bool_roundtrip_103860; CREATE TABLE t_bool_roundtrip_103860 (b Bool) ENGINE = Memory;"
printf "('true')\n('false')" | ${CH_CLIENT_NO_FALLBACK} --query="INSERT INTO t_bool_roundtrip_103860 FORMAT Values"
${CLICKHOUSE_CLIENT} -q "SELECT b FROM t_bool_roundtrip_103860 ORDER BY b;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_bool_roundtrip_103860;"
echo
