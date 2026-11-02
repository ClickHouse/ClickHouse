#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Under `column_and_query_name_matching = 'standard'` a double quote pins a name to exact matching, so
# the AST JSON form (`parseQueryToJSON`, `formatQueryFromJSON`, `dialect = 'clickhouse_json'`) must keep
# the quote style of identifiers, aliases and other names to stand for the same query.

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_json_quotes (x Int32) ENGINE = Memory; INSERT INTO t_json_quotes VALUES (1), (2), (3)"

echo '--- the JSON keeps double quotes of identifier parts, aliases, CTE and window names'
${CLICKHOUSE_CLIENT} --query "
    WITH parseQueryToJSON('WITH \"C\" AS (SELECT 1 AS \"A\") SELECT t.\"x\", count() OVER \"W\" FROM \"C\", t WINDOW \"W\" AS ()') AS json
    SELECT
        position(json, '\"part_quotes\":[\"unquoted\",\"double_quoted\"]') > 0,
        position(json, '\"alias_quote\":\"double_quoted\"') > 0,
        position(json, '\"name\":\"C\",\"name_quote\":\"double_quoted\"') > 0,
        position(json, '\"window_name_quote\":\"double_quoted\"') > 0"

echo '--- a query run from its JSON form binds like the SQL text'
# `ORDER BY X` folds to the column `x` (the double-quoted alias is pinned), `ORDER BY "X"` is the alias.
for query in 'SELECT -x AS "X" FROM t_json_quotes ORDER BY X' 'SELECT -x AS "X" FROM t_json_quotes ORDER BY "X"'
do
    json=$(${CLICKHOUSE_CLIENT} --param_q "$query" --query "SELECT parseQueryToJSON({q:String}) FORMAT TSVRaw")
    ${CLICKHOUSE_CLIENT} --column_and_query_name_matching=standard --enable_json_ast_dialect 1 --dialect clickhouse_json --query "$json" | tr '\n' ' '
    echo
done
