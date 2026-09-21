#!/usr/bin/env bash
# Tags: no-parallel-replicas
# Tag no-parallel-replicas -- the test manages parallel replicas settings itself

# Regression test for "Initiator received more initial requests than there are replicas:
# replica_num=N" and its sibling "Duplicate announcement received for replica number N"
# (parallel replicas coordinator). Found by the AST fuzzer.
#
# mergeTreeAnalyzeIndexes keeps its predicate as an AST and resolves it at execution time with
# resolveConstantExpression; addQueryTreePasses does not run on that tree, so
# DisableParallelReplicasPass never saw the predicate, and a correlated IN subquery inside it was
# distributed to the replicas. Decorrelating that subquery on a replica materializes the referenced
# subplan a second time, so one table is read twice in one plan, both reads share a stream_id and
# the replica announces twice on the same coordinator.
#
# Each assertion reports the analysis result as well as the coordinator count: a coordinator
# count of 0 on its own would also be produced by a query that failed outright.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_pr_ai;
DROP TABLE IF EXISTS t_pr_ai_2;
CREATE TABLE t_pr_ai   (key Int32) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t_pr_ai_2 (key Int32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_pr_ai   SELECT number FROM numbers(100);
INSERT INTO t_pr_ai_2 SELECT number      FROM numbers(25);
INSERT INTO t_pr_ai_2 SELECT number + 25 FROM numbers(25);
INSERT INTO t_pr_ai_2 SELECT number + 50 FROM numbers(25);
INSERT INTO t_pr_ai_2 SELECT number + 75 FROM numbers(25);
"

# correlated_subqueries_use_in_memory_buffer = 0 is what a lowered compatibility setting gives;
# with the in-memory buffer the decorrelated plan does not clone the referenced read.
pr_settings="SETTINGS enable_analyzer = 1, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', correlated_subqueries_use_in_memory_buffer = 0"

# Reports whether the analysis produced rows, and the coordinators it created: exactly, when
# none are expected, otherwise as a boolean, because the count of a query that legitimately
# distributes depends on the plan the randomized settings produce.
# The row count of mergeTreeAnalyzeIndexes follows the number of active parts, which randomized
# insert settings may change, so the analysis is asserted to be non-empty rather than exact.
report() {
    local label="$1" query="$2" expect="$4"
    local query_id="05211_${CLICKHOUSE_DATABASE}_$3"
    local res ok=0
    res=$(${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "$query $pr_settings" 2>&1)
    [[ $res =~ ^[0-9]+$ ]] && [ "$res" -ge 1 ] && ok=1
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
    local coordinators
    coordinators=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count()
        FROM system.text_log
        WHERE query_id = '$query_id'
          AND message LIKE '%Creating parallel replicas coordinator%'")
    if [ "$expect" = positive ]; then
        echo "$label: analysis ok $ok, any coordinator $(( coordinators > 0 ? 1 : 0 ))"
    else
        echo "$label: analysis ok $ok, coordinators $coordinators"
    fi
}

correlated="SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_pr_ai, (key IN (SELECT (SELECT key) FROM t_pr_ai_2 ORDER BY key LIMIT 10)))"

# The predicate's correlated subquery must not be distributed to the replicas. Expect 0.
report 'correlated predicate' "$correlated" correlated zero

# A SETTINGS clause on the subquery itself must not re-enable parallel replicas: the pass runs
# after the query tree builder has applied that clause to the subquery's own context. Expect 0.
report 'correlated predicate, subquery SETTINGS' \
    "SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_pr_ai, (key IN (SELECT (SELECT key) FROM t_pr_ai_2 ORDER BY key LIMIT 10 SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost')))" \
    subquery_settings zero

# The correlated subquery one level deeper: the contexts of nested query nodes are read
# separately by the planner, so all of them have to be covered. Expect 0.
report 'correlated predicate, nested one level deeper' \
    "SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_pr_ai, (key IN (SELECT key FROM (SELECT (SELECT key) AS key FROM t_pr_ai_2) ORDER BY key LIMIT 10)))" \
    nested zero

# Control: an uncorrelated predicate subquery still uses parallel replicas, so the fix did not
# reintroduce a blanket disable for this table function. Expect a coordinator.
report 'uncorrelated predicate' \
    "SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_pr_ai, (key IN (SELECT key FROM t_pr_ai_2)))" \
    uncorrelated positive

# Control: parallel replicas are available at all in this configuration. Without it every
# assertion above would also hold on a server that cannot use parallel replicas.
# An aggregate over a column, not count(), so that the query is not answered from part metadata.
plain_id="05211_${CLICKHOUSE_DATABASE}_plain"
${CLICKHOUSE_CLIENT} --query_id "$plain_id" --query "SELECT sum(key) FROM t_pr_ai_2 $pr_settings" > /dev/null
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
echo -n 'plain query uses parallel replicas: '
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0
    FROM system.text_log
    WHERE query_id = '$plain_id'
      AND message LIKE '%Creating parallel replicas coordinator%'"

# The analysis must return the same ranges with and without parallel replicas.
echo -n 'correlated predicate result matches non-parallel: '
with_pr=$(${CLICKHOUSE_CLIENT} --query "SELECT part_name, ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_pr_ai, (key IN (SELECT (SELECT key) FROM t_pr_ai_2 ORDER BY key LIMIT 10))) ORDER BY part_name $pr_settings")
without_pr=$(${CLICKHOUSE_CLIENT} --query "SELECT part_name, ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_pr_ai, (key IN (SELECT (SELECT key) FROM t_pr_ai_2 ORDER BY key LIMIT 10))) ORDER BY part_name SETTINGS enable_analyzer = 1, enable_parallel_replicas = 0, correlated_subqueries_use_in_memory_buffer = 0")
if [ -n "$with_pr" ] && [ "$with_pr" = "$without_pr" ]; then echo 1; else echo 0; fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_pr_ai; DROP TABLE t_pr_ai_2"
