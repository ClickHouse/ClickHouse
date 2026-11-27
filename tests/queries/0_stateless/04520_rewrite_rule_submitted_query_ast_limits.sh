#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A rewrite rule can match a large source query and rewrite it to a tiny one. The AST size/depth
# limits (`max_ast_elements` / `max_ast_depth`) must be enforced on the query as the user
# submitted it, before the matcher runs: otherwise the generic post-rewrite `checkASTSizeLimits`
# in `executeQuery` only sees the small rewrite result, letting a user with a low limit bypass the
# AST resource guard for the (oversized) query they actually sent.

# The rule name is global; make it unique per test database so the test is parallel-safe.
# `DROP RULE` has no `IF EXISTS` form, so a leftover from a previous failed run is dropped
# defensively.
RULE="rule_submitted_ast_limit_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}" 2>/dev/null

# Created under default limits (well above this small template), the rule rewrites a 20-column
# SELECT to a tiny one.
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20) REWRITE TO (SELECT 42)"

# With a low element limit and the rule active, the submitted (oversized) query must be rejected
# before matching, so the rule cannot be used to bypass `max_ast_elements`.
echo "submitted oversized query with low max_ast_elements:"
$CLICKHOUSE_CLIENT --query_rules "${RULE}" --max_ast_elements 8 --query "SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20" 2>&1 | grep -o -m1 "TOO_BIG_AST" || echo "not rejected"

# The same query is accepted and rewritten once the limit is relaxed: the rule itself is fine and
# the result template stays bounded by the post-rewrite check.
echo "same query with the default limit (rewritten to the small result):"
$CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"
