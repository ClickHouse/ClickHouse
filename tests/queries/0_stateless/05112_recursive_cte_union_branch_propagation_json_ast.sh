#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A branch of an INTERSECT/EXCEPT node can receive the first branch's WITH list only when the AST
# already holds that node. SQL text never does: the propagation runs before INTERSECT/EXCEPT is
# rewritten into ASTSelectIntersectExceptQuery, so from SQL the propagation always sees a flat
# ASTSelectWithUnionQuery. Deserializing the AST from JSON is what puts such a node in front of the
# propagation, which makes `dialect = clickhouse_json` the only way to cover that target.

# UNION ALL of the branch owning the WITH list and an INTERSECT node whose first operand has no WITH
# of its own and so receives a copy of the list. $1 is the query the two branches are taken from, $2
# the second INTERSECT operand, equal to the first so the intersection does not depend on it, $3 the
# analyzer the JSON AST is executed with.
run_json_ast()
{
    local json
    json=$(${CLICKHOUSE_LOCAL} -q "
        WITH parseQueryToJSON('$1') AS u
        SELECT concat(
            '{\"type\":\"SelectWithUnionQuery\",\"union_mode\":\"UNION_DEFAULT\",\"list_of_modes\":[\"UNION_ALL\"],\"list_of_selects\":{\"type\":\"ExpressionList\",\"children\":[',
            JSONExtractRaw(u, 'list_of_selects', 'children', 1), ',',
            '{\"type\":\"SelectIntersectExceptQuery\",\"final_operator\":\"INTERSECT ALL\",\"children\":[',
            JSONExtractRaw(u, 'list_of_selects', 'children', 2), ',', parseQueryToJSON('SELECT $2 AS s'), ']}]}}')
        FORMAT TSVRaw")
    ${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json --enable_analyzer "$3" -q "$json"
}

RECURSIVE_BRANCHES="WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM src WHERE id < 3) SELECT sum(id) AS s FROM src UNION ALL SELECT sum(id) AS s FROM src"
PLAIN_BRANCHES="WITH src AS (SELECT 1 AS id UNION ALL SELECT 2) SELECT sum(id) AS s FROM src UNION ALL SELECT sum(id) AS s FROM src"

# Both branches sum the ids 1, 2, 3: the copy keeps RECURSIVE, so the self-reference inside it still
# resolves to the recursive CTE. Without the flag the copy is an ordinary CTE and the operand fails
# with UNKNOWN_TABLE on that self-reference.
run_json_ast "$RECURSIVE_BRANCHES" 6 1

# A non-recursive list carries no flag to lose, so this arm holds while the one above moves.
run_json_ast "$PLAIN_BRANCHES" 3 1

# The flag must not be invented on the copy either, and the old analyzer reads it: it rejects a
# recursive WITH outright. The recursive arm is refused by its own source branch, which the union
# interpreter builds first, so that arm only shows the refusal is reachable for this shape. The
# non-recursive arm is the oracle: it keeps running only while the copy stays non-recursive.
run_json_ast "$RECURSIVE_BRANCHES" 6 0 2>&1 | grep -om1 UNSUPPORTED_METHOD
run_json_ast "$PLAIN_BRANCHES" 3 0
