#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the pre-match and post-rewrite AST size/depth guards (and the rule-template limit
# check) used `IAST::checkSize` / `checkDepth`, which follow only `children`. The matcher's tree
# hash now folds in semantic subtrees kept OUTSIDE `children` (a `SHOW ... WHERE`, a `BACKUP`
# setting, a `ROW POLICY` filter), so a query whose bulk lives in such a member slipped past the
# `max_ast_elements` guard yet still forced the matcher to walk an unbounded tree. The guards now
# count those members too.
#
# A `SHOW TABLES WHERE <deep expression>` (the WHERE lives in `ASTShowTablesQuery::where_expression`,
# not `children`) is submitted with a rule active:
#   * with a generous `max_ast_elements` it runs (the rule does not match a SHOW);
#   * with a small `max_ast_elements` it is rejected as too big, because the WHERE is now counted.
# The rule name is suffixed with the test database so the test stays parallel-safe.

DB="${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "CREATE RULE rule_limit_${DB} AS (SELECT 987654) REWRITE TO (SELECT 987654)"

WHERE_CLAUSE="name = 'a' AND name = 'b' AND name = 'c' AND name = 'd' AND name = 'e' AND name = 'f'"

check() # $1 max_ast_elements, $2 label
{
    out=$($CLICKHOUSE_CLIENT --query_rules="rule_limit_${DB}" --max_ast_elements="$1" --query "SHOW TABLES WHERE ${WHERE_CLAUSE}" 2>&1 || true)
    if echo "$out" | grep -q "TOO_BIG_AST"; then echo "$2: too big"; else echo "$2: accepted"; fi
}

check 1000 "generous limit"
check 8 "small limit"

$CLICKHOUSE_CLIENT --query "DROP RULE rule_limit_${DB}"
