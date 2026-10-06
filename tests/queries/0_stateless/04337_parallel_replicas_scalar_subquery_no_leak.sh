#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# Tag no-parallel -- queries system.text_log
# Tag no-parallel-replicas -- the test manages parallel replicas settings itself

# Regression test for "Duplicate announcement received for replica number 1"
# (parallel replicas coordinator). Found by the AST fuzzer.
#
# A scalar subquery must never use parallel replicas: evaluateScalarSubqueryIfNeeded
# disabled them only on the top node of the cloned subquery tree, so nested
# QueryNode/UnionNode children kept parallel replicas enabled. The scalar's plan was
# then built with parallel replicas during analysis, creating coordinator instances;
# when the same table was read twice in one local plan they announced on the same
# coordinator and tripped the duplicate-announcement assertion.
#
# We assert that evaluating a scalar subquery over a parallel-replicas table creates
# no coordinator (a top-level query over the same data still does, so parallel
# replicas keeps working). The coordinator count is read from system.text_log.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_pr_scalar;
CREATE TABLE t_pr_scalar (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_scalar SELECT number % 100, number FROM numbers(1000);
"

pr_settings="SETTINGS enable_analyzer = 1, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost'"

# Number of "Creating parallel replicas coordinator" messages logged while running $1.
coordinators_for() {
    local query_id="04337_${CLICKHOUSE_DATABASE}_$2"
    ${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "$1 $pr_settings" >/dev/null 2>&1
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count()
        FROM system.text_log
        WHERE query_id = '$query_id'
          AND message LIKE '%Creating parallel replicas coordinator%'"
}

# Scalar subquery over a nested subquery -- before the fix the nested QueryNode kept
# parallel replicas enabled, so the scalar was planned with parallel replicas. Expect 0.
echo -n 'scalar over nested subquery, coordinators: '
coordinators_for "SELECT (SELECT count() FROM (SELECT a FROM t_pr_scalar))" nested

# Scalar subquery whose nested child is a UNION over the same table -- two same-table
# reads in one leaked plan share a coordinator and tripped the assertion. Expect 0.
echo -n 'scalar over nested union, coordinators: '
coordinators_for "SELECT (SELECT count() FROM (SELECT a FROM t_pr_scalar UNION ALL SELECT a FROM t_pr_scalar))" union

# Control: a top-level query over the same data still uses parallel replicas. Expect > 0.
echo -n 'top-level query uses parallel replicas: '
top_id="04337_${CLICKHOUSE_DATABASE}_top"
${CLICKHOUSE_CLIENT} --query_id "$top_id" --query "SELECT count() FROM (SELECT a FROM t_pr_scalar UNION ALL SELECT a FROM t_pr_scalar) $pr_settings" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0
    FROM system.text_log
    WHERE query_id = '$top_id'
      AND message LIKE '%Creating parallel replicas coordinator%'"

# The scalar subqueries must also still return correct results.
echo -n 'scalar results correct: '
${CLICKHOUSE_CLIENT} --query "
    SELECT
        (SELECT count() FROM (SELECT a FROM t_pr_scalar)) = 1000
        AND (SELECT count() FROM (SELECT a FROM t_pr_scalar UNION ALL SELECT a FROM t_pr_scalar)) = 2000
    $pr_settings"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_pr_scalar"
