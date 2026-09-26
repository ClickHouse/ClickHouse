#!/usr/bin/env bash
# Tags: no-random-settings

# Regression test: constant folded from UNION (INTERSECT ALL) node should not
# cause "Invalid action query tree node" exception in `calculateActionNodeName`.
# The bug requires a constant with UNION source expression in a context where
# `isASTLevelOptimizationAllowed` returns false (SECONDARY_QUERY context).
# Using --query_kind=secondary_query directly simulates a shard receiving
# a distributed query, which is how the bug was originally found by the AST
# fuzzer in the stress test (TSan).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The actual bug reproducer: --query_kind=secondary_query makes
# isASTLevelOptimizationAllowed() return false, so the planner tries to
# generate an action name from the source expression of the constant-folded
# INTERSECT ALL (a UNION node). Without the fix, this throws
# "Invalid action query tree node".
$CLICKHOUSE_CLIENT --query_kind=secondary_query -q "SELECT (SELECT 1 INTERSECT ALL SELECT 1)"
$CLICKHOUSE_CLIENT --query_kind=secondary_query -q "SELECT (SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10)))"

# Also test that the queries work normally (initial_query context).
$CLICKHOUSE_CLIENT -q "SELECT (SELECT 1 INTERSECT ALL SELECT 1)"
$CLICKHOUSE_CLIENT -q "SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10))"
