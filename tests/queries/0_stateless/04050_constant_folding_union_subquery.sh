#!/usr/bin/env bash
# Reproducer for exception in PlannerActionsVisitor when a constant is folded
# from a UNION/EXCEPT ALL subquery (the source expression is a UnionNode).
# The --query_kind=secondary_query flag sets SECONDARY_QUERY context where
# isASTLevelOptimizationAllowed() is false, which is needed to reach the buggy code path.
# The EXCEPT ALL / UNION ALL must be inside a scalar subquery so that constant
# folding produces a ConstantNode with a UnionNode source expression.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query_kind=secondary_query --query "SELECT (SELECT toUInt256(7) EXCEPT ALL SELECT 0 EXCEPT ALL SELECT 2147483646)"
${CLICKHOUSE_CLIENT} --query_kind=secondary_query --query "SELECT (SELECT 1 EXCEPT ALL SELECT 2)"
