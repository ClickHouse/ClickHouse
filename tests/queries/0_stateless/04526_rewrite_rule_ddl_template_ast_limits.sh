#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A submitted `CREATE RULE` / `ALTER RULE` keeps its rule templates (`source_query` /
# `resulting_query`) outside `IAST::children`. The per-query AST size/depth guard that runs before
# the rewrite-rule matcher walks `children` only, so with `query_rules` active the matcher would
# hash/walk an oversized submitted rule template before the generic `checkRewriteRuleTemplateLimits`
# gate in `executeQuery`. The template limits are now also enforced inside `astTraversal`, before the
# matcher, whenever rules are active — so an oversized submitted rule DDL is bounded regardless of
# which rules are active.

# Rule names are global; make them unique per test database so the test is parallel-safe.
ACTIVE="rule_active_${CLICKHOUSE_DATABASE}"
BIG="rule_big_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP RULE ${ACTIVE}" 2>/dev/null
$CLICKHOUSE_CLIENT --query "DROP RULE ${BIG}" 2>/dev/null

# A small, valid rule so `query_rules` has an active rule the matcher runs against.
$CLICKHOUSE_CLIENT --query "CREATE RULE ${ACTIVE} AS (SELECT 'nomatch') REWRITE TO (SELECT 1)"

# With that rule active and a low element limit, submitting a `CREATE RULE` whose source template is
# oversized must be rejected before the matcher hashes/walks it.
echo "oversized submitted CREATE RULE template with a low max_ast_elements:"
$CLICKHOUSE_CLIENT --query_rules "${ACTIVE}" --max_ast_elements 8 --query \
  "CREATE RULE ${BIG} AS (SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20) REWRITE TO (SELECT 1)" \
  2>&1 | grep -o -m1 "TOO_BIG_AST" || echo "not rejected"

# The oversized template of an `ALTER RULE` is bounded the same way.
$CLICKHOUSE_CLIENT --query "CREATE RULE ${BIG} AS (SELECT 0) REWRITE TO (SELECT 1)"
echo "oversized submitted ALTER RULE template with a low max_ast_elements:"
$CLICKHOUSE_CLIENT --query_rules "${ACTIVE}" --max_ast_elements 8 --query \
  "ALTER RULE ${BIG} AS (SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20) REWRITE TO (SELECT 1)" \
  2>&1 | grep -o -m1 "TOO_BIG_AST" || echo "not rejected"

# With the default limit the same template is accepted: the rule itself is small enough, only the
# artificially low limit rejected it.
$CLICKHOUSE_CLIENT --query "DROP RULE ${BIG}"
echo "same CREATE RULE template with the default limit (accepted):"
$CLICKHOUSE_CLIENT --query_rules "${ACTIVE}" --query \
  "CREATE RULE ${BIG} AS (SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20) REWRITE TO (SELECT 1)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.query_rules WHERE name = '${BIG}'"

$CLICKHOUSE_CLIENT --query "DROP RULE ${BIG}"
$CLICKHOUSE_CLIENT --query "DROP RULE ${ACTIVE}"
