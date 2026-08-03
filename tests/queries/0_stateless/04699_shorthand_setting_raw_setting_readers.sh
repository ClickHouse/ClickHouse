#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The valueless `SETTINGS name` form stands for `name = true`, and the SQL parser always writes
# Bool `true` for it. The AST JSON dialect can pair the `shorthand` flag with any other value.
# `BaseSettings` rejects such a change, but `SettingsChanges` are also consumed raw - the `Join`
# engine and `EXPLAIN` settings read `SettingChange::value` directly - so the mismatch is rejected
# once for the whole query tree, before any raw reader executes the carried value.

# A crafted payload for the `Join` engine: `shorthand` claims the valueless form while carrying
# `false` for `persistent`.
CRAFTED_JOIN="replaceAll(parseQueryToJSON(\$\$CREATE TABLE test_04699 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent = false\$\$), '{\"name\":\"persistent\"', '{\"name\":\"persistent\",\"shorthand\":true')"
CRAFTED_JOIN_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_JOIN FORMAT TSVRaw")
$CLICKHOUSE_CLIENT --dialect clickhouse_json --allow_experimental_json_ast_dialect 1 -q "$CRAFTED_JOIN_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# The same for `EXPLAIN` settings, whose parser does not even accept the valueless form, so any
# shorthand flag there is parser-impossible.
CRAFTED_EXPLAIN="replaceAll(parseQueryToJSON(\$\$EXPLAIN header = 1 SELECT 1\$\$), '{\"name\":\"header\"', '{\"name\":\"header\",\"shorthand\":true')"
CRAFTED_EXPLAIN_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_EXPLAIN FORMAT TSVRaw")
$CLICKHOUSE_CLIENT --dialect clickhouse_json --allow_experimental_json_ast_dialect 1 -q "$CRAFTED_EXPLAIN_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# The genuine valueless form for a Bool engine setting is untouched: it round-trips through the
# JSON dialect and still executes.
GENUINE_JSON=$($CLICKHOUSE_CLIENT -q "SELECT parseQueryToJSON(\$\$CREATE TABLE test_04699 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent\$\$) FORMAT TSVRaw")
$CLICKHOUSE_CLIENT --dialect clickhouse_json --allow_experimental_json_ast_dialect 1 -q "$GENUINE_JSON"
$CLICKHOUSE_CLIENT -q "SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 'test_04699'"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_04699"
