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
