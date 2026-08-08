#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The valueless `SETTINGS name` form stands for `name = true`, and the SQL parser always writes
# Bool `true` for it. The AST JSON dialect can pair the `shorthand` flag with any other value.
# `BaseSettings` rejects such a change, but `SettingsChanges` are also consumed raw - the `Join`
# engine and `EXPLAIN` settings read `SettingChange::value` directly - so the mismatch is rejected
# once for the whole query tree, at every `IAST::createFromJSON` entry point: the server rejects it
# before any raw reader executes the carried value, and the client rejects it at parse, before any
# AST-to-SQL rewrite (query-parameter substitution for old servers, `allow_merge_tree_settings`)
# can serialize the malformed tree into executable SQL. The crafted payloads are sent over HTTP so
# they reach the server verbatim and exercise the server-side check; the client-side check is
# exercised separately at the end.

# A crafted payload for the `Join` engine: `shorthand` claims the valueless form while carrying
# `false` for `persistent`.
CRAFTED_JOIN="replaceAll(parseQueryToJSON(\$\$CREATE TABLE test_04699 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent = false\$\$), '{\"name\":\"persistent\"', '{\"name\":\"persistent\",\"shorthand\":true')"
CRAFTED_JOIN_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_JOIN FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1" --data-binary "$CRAFTED_JOIN_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# The same for `EXPLAIN` settings, whose parser does not even accept the valueless form, so any
# shorthand flag there is parser-impossible.
CRAFTED_EXPLAIN="replaceAll(parseQueryToJSON(\$\$EXPLAIN header = 1 SELECT 1\$\$), '{\"name\":\"header\"', '{\"name\":\"header\",\"shorthand\":true')"
CRAFTED_EXPLAIN_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_EXPLAIN FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1" --data-binary "$CRAFTED_EXPLAIN_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# Dictionary settings are stored in `ASTDictionarySettings` rather than in an `ASTSetQuery`,
# and `getDictionaryConfigurationFromAST` reads them raw.
CRAFTED_DICT="replaceAll(parseQueryToJSON(\$\$CREATE DICTIONARY test_04699_dict (k UInt64, v UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'test_04699')) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(check_dictionary_primary_key = 0)\$\$), '{\"name\":\"check_dictionary_primary_key\"', '{\"name\":\"check_dictionary_primary_key\",\"shorthand\":true')"
CRAFTED_DICT_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_DICT FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1" --data-binary "$CRAFTED_DICT_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# WASM function settings are stored in `ASTCreateWasmFunctionQuery` rather than in an
# `ASTSetQuery`, `validateAndGetDefinition` reads them raw, and `formatImpl` would render the
# crafted change in the valueless form, hiding the carried value.
CRAFTED_WASM="replaceAll(parseQueryToJSON(\$\$CREATE FUNCTION test_04699_wasm LANGUAGE WASM FROM 'mod' ARGUMENTS (UInt32) RETURNS UInt32 SETTINGS max_instances = 0\$\$), '{\"name\":\"max_instances\"', '{\"name\":\"max_instances\",\"shorthand\":true')"
CRAFTED_WASM_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_WASM FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1" --data-binary "$CRAFTED_WASM_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# The dictionary and WASM function settings grammar mandates `name = value`, so the valueless
# form is parser-impossible there whatever the value - even the mandatory `true` is rejected.
# For a WASM function letting it through would persist `SETTINGS max_instances` (the temporary
# `ASTSetQuery` used by `formatImpl` elides `= true` for the valueless form), which the SQL
# grammar then fails to parse back when the function is reloaded.
CRAFTED_WASM_TRUE="replaceAll(parseQueryToJSON(\$\$CREATE FUNCTION test_04699_wasm LANGUAGE WASM FROM 'mod' ARGUMENTS (UInt32) RETURNS UInt32 SETTINGS webassembly_udf_enable_fuel = true\$\$), '{\"name\":\"webassembly_udf_enable_fuel\"', '{\"name\":\"webassembly_udf_enable_fuel\",\"shorthand\":true')"
CRAFTED_WASM_TRUE_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_WASM_TRUE FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1" --data-binary "$CRAFTED_WASM_TRUE_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

CRAFTED_DICT_TRUE="replaceAll(parseQueryToJSON(\$\$CREATE DICTIONARY test_04699_dict (k UInt64, v UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'test_04699')) LAYOUT(FLAT()) LIFETIME(0) SETTINGS(check_dictionary_primary_key = 1)\$\$), '{\"name\":\"check_dictionary_primary_key\"', '{\"name\":\"check_dictionary_primary_key\",\"shorthand\":true')"
CRAFTED_DICT_TRUE_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_DICT_TRUE FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1" --data-binary "$CRAFTED_DICT_TRUE_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# Column-level `SETTINGS (...)` and `EXPLAIN` settings ride in an `ASTSetQuery`, but their
# grammar disables the valueless form, so the flag is parser-impossible there even with the
# mandatory `true` - and nothing in these paths consults it: `MergeTreeColumnSettings::validate`
# only type-checks the value, so a surviving flag would be persisted in the table definition as
# `SETTINGS (min_compress_block_size)` that the column grammar then fails to parse back.
CRAFTED_COLUMN="replaceAll(parseQueryToJSON(\$\$CREATE TABLE test_04699_column (k UInt64, v UInt64 SETTINGS (min_compress_block_size = true)) ENGINE = MergeTree ORDER BY k\$\$), '{\"name\":\"min_compress_block_size\"', '{\"name\":\"min_compress_block_size\",\"shorthand\":true')"
CRAFTED_COLUMN_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_COLUMN FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1" --data-binary "$CRAFTED_COLUMN_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

CRAFTED_EXPLAIN_TRUE="replaceAll(parseQueryToJSON(\$\$EXPLAIN header = true SELECT 1\$\$), '{\"name\":\"header\"', '{\"name\":\"header\",\"shorthand\":true')"
CRAFTED_EXPLAIN_TRUE_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_EXPLAIN_TRUE FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1" --data-binary "$CRAFTED_EXPLAIN_TRUE_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# `BACKUP`/`RESTORE` settings also ride in an `ASTSetQuery`, but `ParserBackupQuery` accepts only
# `name = value` pairs, so the flag is parser-impossible there too - even with the mandatory
# `true`. It is not just a formatting hazard: `BackupSettings` and `RestoreSettings` read
# `SettingChange::value` raw, so on a numeric setting a surviving flag would both execute
# (`fieldToNumber` coerces Bool `true` to `1`) and format as `SETTINGS compression_level` that
# the backup grammar fails to parse back. The check fires before the backup is attempted, so the
# target does not need to exist.
CRAFTED_BACKUP="replaceAll(parseQueryToJSON(\$\$BACKUP TABLE test_04699 TO Disk('backups', '04699') SETTINGS deduplicate_files = true\$\$), '{\"name\":\"deduplicate_files\"', '{\"name\":\"deduplicate_files\",\"shorthand\":true')"
CRAFTED_BACKUP_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_BACKUP FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1" --data-binary "$CRAFTED_BACKUP_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

CRAFTED_RESTORE="replaceAll(parseQueryToJSON(\$\$RESTORE TABLE test_04699 FROM Disk('backups', '04699') SETTINGS allow_non_empty_tables = true\$\$), '{\"name\":\"allow_non_empty_tables\"', '{\"name\":\"allow_non_empty_tables\",\"shorthand\":true')"
CRAFTED_RESTORE_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_RESTORE FORMAT TSVRaw")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1" --data-binary "$CRAFTED_RESTORE_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# The client parses the `clickhouse_json` dialect with the same `IAST::createFromJSON` and applies
# the same check at parse. This closes the client's AST-to-SQL rewrite paths: e.g. under
# `--allow_merge_tree_settings` with changed MergeTree settings (the test harness randomization),
# `ClientBase::addMergeTreeSettings` rewrites a parsed `CREATE ... MergeTree` and re-sends it as
# SQL text, where the crafted `Join` change would serialize as executable `persistent = false` and
# the crafted column-level change as `SETTINGS (min_compress_block_size)` that the column grammar
# rejects with a syntax error - so without the parse-time check the rewrite would execute or
# re-brand the malformed payload instead of rejecting it.
$CLICKHOUSE_CLIENT --dialect clickhouse_json --enable_json_ast_dialect 1 -q "$CRAFTED_JOIN_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1
$CLICKHOUSE_CLIENT --dialect clickhouse_json --enable_json_ast_dialect 1 -q "$CRAFTED_COLUMN_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# `formatQueryFromJSON` is the remaining `createFromJSON` consumer and applies the same check:
# formatting the malformed tree would either render the valueless form where the SQL grammar does
# not accept it (`SETTINGS webassembly_udf_enable_fuel`, which `ParserCreateFunctionQuery` cannot
# parse back), or serialize the carried value into SQL that no longer carries the malformed flag.
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON($CRAFTED_WASM_TRUE)" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON($CRAFTED_JOIN)" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# The genuine valueless form for a Bool engine setting is untouched: it round-trips through the
# JSON dialect and still executes.
GENUINE_JSON=$($CLICKHOUSE_CLIENT -q "SELECT parseQueryToJSON(\$\$CREATE TABLE test_04699 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent\$\$) FORMAT TSVRaw")
$CLICKHOUSE_CLIENT --dialect clickhouse_json --enable_json_ast_dialect 1 -q "$GENUINE_JSON"
$CLICKHOUSE_CLIENT -q "SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 'test_04699'"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_04699"
