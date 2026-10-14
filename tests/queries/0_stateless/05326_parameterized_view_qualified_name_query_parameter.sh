#!/usr/bin/env bash
# A qualified value for an `Identifier` query parameter must resolve a parameterized
# view by database and name on every path that reads `ASTFunction::isCompoundName()`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
CREATE TABLE data (id UInt64) ENGINE = Memory;
INSERT INTO data VALUES (1), (2);
CREATE VIEW filtered AS SELECT * FROM data WHERE id = {id:UInt64};
"

VIEW="${CLICKHOUSE_DATABASE}.filtered"

$CLICKHOUSE_CLIENT --param_view="$VIEW" -q "SELECT id FROM {view:Identifier}(id = 2) SETTINGS enable_analyzer = 0"
$CLICKHOUSE_CLIENT --param_view="$VIEW" -q "SELECT id FROM {view:Identifier}(id = 2) SETTINGS enable_analyzer = 1"
$CLICKHOUSE_CLIENT --param_db="$CLICKHOUSE_DATABASE" --param_name=filtered -q "SELECT id FROM {db:Identifier}.{name:Identifier}(id = 1)"
$CLICKHOUSE_CLIENT --param_view="$VIEW" --param_arg=id --param_value=2 -q "SELECT id FROM {view:Identifier}({arg:Identifier} = {value:UInt64})"
# The formatter prints a resolved compound name as one quoted identifier; it must still resolve when reparsed.
$CLICKHOUSE_CLIENT -q "SELECT id FROM \`${CLICKHOUSE_DATABASE}.filtered\`(id = 2)"
$CLICKHOUSE_CLIENT --param_view="$VIEW" -q "EXPLAIN SYNTAX SELECT * FROM {view:Identifier}(id = 1)" | sed "s/${CLICKHOUSE_DATABASE}/default/g"

# The same query submitted as a JSON AST resolves the placeholder on the table expression.
JSON=$($CLICKHOUSE_CLIENT -q "SELECT parseQueryToJSON('SELECT id FROM {view:Identifier}(id = {value:UInt64})')")
$CLICKHOUSE_CLIENT --enable_json_ast_dialect=1 --dialect=clickhouse_json --param_view="$VIEW" --param_value=1 -q "$JSON"

# Only a table expression may carry a parameterized function name. A JSON AST can put one on a scalar
# function, which the SQL parser never does, so the JSON reader must reject it at the boundary.
JSON=$($CLICKHOUSE_CLIENT -q "SELECT replaceOne(parseQueryToJSON('SELECT f(1)'), '\"name\":\"f\"', '\"name\":\"\",\"query_parameter_name\":{\"type\":\"Identifier\",\"name\":\"\",\"children\":[{\"type\":\"QueryParameter\",\"name\":\"view\",\"param_type\":\"Identifier\"}]}')")
$CLICKHOUSE_CLIENT --enable_json_ast_dialect=1 --dialect=clickhouse_json --param_view=plus -q "$JSON" 2>&1 | grep -q BAD_ARGUMENTS && echo "scalar function name rejected"
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON('$JSON')" 2>&1 | grep -q BAD_ARGUMENTS && echo "scalar function name rejected by formatQueryFromJSON"

# The SQL parser accepts only `{name:Identifier}` inside an identifier, so the JSON reader must reject any other type.
JSON=$($CLICKHOUSE_CLIENT -q "SELECT replaceOne(parseQueryToJSON('SELECT id FROM {view:Identifier}(id = 1)'), '\"param_type\":\"Identifier\"', '\"param_type\":\"String\"')")
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON('$JSON')" 2>&1 | grep -q BAD_ARGUMENTS && echo "non-Identifier parameter type rejected"

# An alias on the name identifier or on its placeholder would format as `{view:Identifier} AS a(...)`, which does not parse back.
JSON=$($CLICKHOUSE_CLIENT -q "SELECT replaceOne(parseQueryToJSON('SELECT id FROM {view:Identifier}(id = 1)'), '\"query_parameter_name\":{\"type\":\"Identifier\"', '\"query_parameter_name\":{\"type\":\"Identifier\",\"alias\":\"a\"')")
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON('$JSON')" 2>&1 | grep -q BAD_ARGUMENTS && echo "alias on the name rejected"
$CLICKHOUSE_CLIENT --enable_json_ast_dialect=1 --dialect=clickhouse_json --param_view=numbers -q "$JSON" 2>&1 | grep -q BAD_ARGUMENTS && echo "alias on the name rejected by the dialect"
JSON=$($CLICKHOUSE_CLIENT -q "SELECT replaceOne(parseQueryToJSON('SELECT id FROM {view:Identifier}(id = 1)'), '\"type\":\"QueryParameter\",\"name\":\"view\"', '\"type\":\"QueryParameter\",\"alias\":\"a\",\"name\":\"view\"')")
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON('$JSON')" 2>&1 | grep -q BAD_ARGUMENTS && echo "alias on the placeholder rejected"
