#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The valueless `SETTINGS name` form stands for `name = true`, and the parser always writes Bool
# `true` for it. The AST JSON dialect can pair the `shorthand` flag with any other value, and for a
# `Bool` setting the type check alone accepted that: the query executed with the carried value while
# every formatter rendered the bare name, so the logged query under-reported what ran.

# Craft a payload for a `Bool` setting whose shorthand flag carries `false`.
CRAFTED="replaceAll(parseQueryToJSON(\$\$SELECT 1 SETTINGS optimize_move_to_prewhere = false\$\$), '{\"name\":\"optimize_move_to_prewhere\"', '{\"name\":\"optimize_move_to_prewhere\",\"shorthand\":true')"

# The formatter must not claim the setting was written without a value when it carries `false`.
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON($CRAFTED)"

# And executing it must be rejected instead of silently running with `false`.
CRAFTED_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED FORMAT TSVRaw")
$CLICKHOUSE_CLIENT --dialect clickhouse_json --allow_experimental_json_ast_dialect 1 -q "$CRAFTED_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# The same for a value of another type smuggled in under the flag: rejected for carrying a value,
# before the type of the setting is even considered.
CRAFTED_STR="replaceAll(parseQueryToJSON(\$\$SELECT 1 SETTINGS optimize_move_to_prewhere = false\$\$), '\"value\":{\"field_type\":\"Bool\",\"value\":false}', '\"shorthand\":true,\"value\":{\"field_type\":\"String\",\"value\":\"x\"}')"
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON($CRAFTED_STR)"
CRAFTED_STR_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED_STR FORMAT TSVRaw")
$CLICKHOUSE_CLIENT --dialect clickhouse_json --allow_experimental_json_ast_dialect 1 -q "$CRAFTED_STR_JSON" 2>&1 | grep -o "BAD_ARGUMENTS" | head -n 1

# The genuine valueless form is untouched: it still formats without a value and still executes.
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(parseQueryToJSON(\$\$SELECT 1 SETTINGS optimize_move_to_prewhere\$\$))"
$CLICKHOUSE_CLIENT -q "SELECT 1 SETTINGS optimize_move_to_prewhere"
$CLICKHOUSE_CLIENT -q "SELECT * FROM (SELECT 1 SETTINGS optimize_move_to_prewhere)"
