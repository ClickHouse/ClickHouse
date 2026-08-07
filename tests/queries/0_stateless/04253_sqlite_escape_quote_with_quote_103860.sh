#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/103860
#
# With `output_format_values_escape_quote_with_quote = 1`, `Values` output for
# `Enum`, `JSON`, and `AggregateFunction` columns used to ignore the setting and
# always emitted `\'`, while `String` and `FixedString` correctly emitted `''`.
# That made `INSERT INTO sqlite_table SELECT enum_col` fail with a SQLite parser
# error whenever the enum value name contained a single quote, because
# `StorageSQLite` forces this setting on its write context.
#
# The fix mirrors the canonical pattern from `SerializationString` /
# `SerializationFixedString`: when the setting is on, escape the apostrophe by
# doubling it (`''`, SQL-standard) instead of backslash-escaping (`\'`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# --- Enum ---
${CLICKHOUSE_CLIENT} -q "SELECT '--- Enum, default (backslash escape) ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "SELECT cast('a\\'b', 'Enum8(\\'a\\'\\'b\\' = 1)') FORMAT Values SETTINGS output_format_values_escape_quote_with_quote = 0;"
echo
${CLICKHOUSE_CLIENT} -q "SELECT '--- Enum, escape_quote_with_quote = 1 (SQL standard) ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "SELECT cast('a\\'b', 'Enum8(\\'a\\'\\'b\\' = 1)') FORMAT Values SETTINGS output_format_values_escape_quote_with_quote = 1;"
echo

# --- JSON (new JSON type) ---
${CLICKHOUSE_CLIENT} -q "SELECT '--- JSON, default ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "SELECT materialize('{\"k\":\"a\\'b\"}')::JSON FORMAT Values SETTINGS output_format_values_escape_quote_with_quote = 0, enable_json_type = 1;"
echo
${CLICKHOUSE_CLIENT} -q "SELECT '--- JSON, escape_quote_with_quote = 1 ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "SELECT materialize('{\"k\":\"a\\'b\"}')::JSON FORMAT Values SETTINGS output_format_values_escape_quote_with_quote = 1, enable_json_type = 1;"
echo

# --- AggregateFunction (binary state); use a custom helper format to keep the
# state binary out of the .reference (the state contains non-printable bytes,
# but its quoted form must escape apostrophes the same way as other types).
# We compare the count of `''` (SQL-standard escape) vs `\'` (backslash escape)
# in the quoted output.
${CLICKHOUSE_CLIENT} -q "SELECT '--- AggregateFunction, default ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "SELECT countSubstrings(s, '\\\\\\'') AS backslash_escapes, countSubstrings(s, '\\'\\'') AS doubled_quote_escapes FROM (SELECT formatRow('Values', groupArrayState(['a\\'b'])) AS s SETTINGS output_format_values_escape_quote_with_quote = 0);"
echo
${CLICKHOUSE_CLIENT} -q "SELECT '--- AggregateFunction, escape_quote_with_quote = 1 ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "SELECT countSubstrings(s, '\\\\\\'') AS backslash_escapes, countSubstrings(s, '\\'\\'') AS doubled_quote_escapes FROM (SELECT formatRow('Values', groupArrayState(['a\\'b'])) AS s SETTINGS output_format_values_escape_quote_with_quote = 1);"
echo

# --- Sanity check: same setting on String must keep working ---
${CLICKHOUSE_CLIENT} -q "SELECT '--- String, escape_quote_with_quote = 1 (sanity) ---' FORMAT LineAsString;"
${CLICKHOUSE_CLIENT} -q "SELECT 'a\\'b' FORMAT Values SETTINGS output_format_values_escape_quote_with_quote = 1;"
echo
