#!/usr/bin/env bash
# Tags: no-parallel-replicas
# Tag no-parallel-replicas -- the test manages parallel replicas settings itself

# Regression test for "Duplicate announcement received for replica number N" (parallel replicas
# coordinator) when a nested SETTINGS clause enables parallel replicas for a correlated subquery
# alone, while the top-level query has them off.
#
# Every (sub)query carries its own context, and the query tree builder applies a nested SETTINGS
# clause to that context, so enable_parallel_replicas can be on for the subquery and off for the
# query around it. Decorrelating such a subquery on a replica materializes the referenced subplan a
# second time, so one table is read twice in one plan, both reads share a stream_id and the replica
# announces twice on the same coordinator.
#
# Each assertion reports the query result as well as the coordinator count: a coordinator count of 0
# on its own would also be produced by a query that failed outright.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_cs_pr;
DROP TABLE IF EXISTS t_cs_pr_2;
CREATE TABLE t_cs_pr   (key Int32) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t_cs_pr_2 (key Int32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_cs_pr   SELECT number FROM numbers(100);
INSERT INTO t_cs_pr_2 SELECT number      FROM numbers(25);
INSERT INTO t_cs_pr_2 SELECT number + 25 FROM numbers(25);
INSERT INTO t_cs_pr_2 SELECT number + 50 FROM numbers(25);
INSERT INTO t_cs_pr_2 SELECT number + 75 FROM numbers(25);
"

# The subquery asks for parallel replicas; the query around it does not.
inner_settings="SETTINGS enable_analyzer = 1, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_min_number_of_rows_per_replica = 0"
# correlated_subqueries_use_in_memory_buffer = 0 is what a lowered compatibility setting gives;
# with the in-memory buffer the decorrelated plan does not clone the referenced read.
outer_settings="SETTINGS enable_analyzer = 1, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 0, correlated_subqueries_use_in_memory_buffer = 0, parallel_replicas_plan_based = 0"

# Reports the query result, and the coordinators it created: exactly, when none are expected,
# otherwise as a boolean, because the count of a query that legitimately distributes depends on the
# plan the randomized settings produce.
report() {
    local label="$1" query="$2" expect="$4"
    local query_id="05233_${CLICKHOUSE_DATABASE}_$3"
    local res
    res=$(${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "$query" 2>&1)
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
    local coordinators
    coordinators=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count()
        FROM system.text_log
        WHERE query_id = '$query_id'
          AND message LIKE '%Creating parallel replicas coordinator%'")
    if [ "$expect" = positive ]; then
        echo "$label: result $res, any coordinator $(( coordinators > 0 ? 1 : 0 ))"
    else
        echo "$label: result $res, coordinators $coordinators"
    fi
}

correlated="SELECT count() FROM t_cs_pr WHERE key IN (SELECT (SELECT key) FROM t_cs_pr_2 ORDER BY key LIMIT 10 $inner_settings) $outer_settings"

# A SETTINGS clause on the subquery itself must not enable parallel replicas for a correlated
# subquery. Expect 10 matching keys and 0 coordinators.
report 'correlated subquery, subquery SETTINGS' "$correlated" correlated zero

# The same shape one level deeper: the contexts of nested query nodes are read separately by the
# planner, so all of them have to be covered. Expect 0.
report 'correlated subquery, nested one level deeper' \
    "SELECT count() FROM t_cs_pr WHERE key IN (SELECT key FROM (SELECT (SELECT key) AS key FROM t_cs_pr_2) ORDER BY key LIMIT 10 $inner_settings) $outer_settings" \
    nested zero

# Control: an uncorrelated subquery with the identical SETTINGS clause still uses parallel replicas,
# so a nested SETTINGS clause did not stop enabling them. Expect a coordinator.
report 'uncorrelated subquery, subquery SETTINGS' \
    "SELECT count() FROM t_cs_pr WHERE key IN (SELECT key FROM t_cs_pr_2 $inner_settings) $outer_settings" \
    uncorrelated positive

# Control: parallel replicas are available at all in this configuration. Without it every assertion
# above would also hold on a server that cannot use parallel replicas.
# An aggregate over a column, not count(), so that the query is not answered from part metadata.
plain_id="05233_${CLICKHOUSE_DATABASE}_plain"
${CLICKHOUSE_CLIENT} --query_id "$plain_id" --query "SELECT sum(key) FROM t_cs_pr_2 $inner_settings" > /dev/null
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
echo -n 'plain query uses parallel replicas: '
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0
    FROM system.text_log
    WHERE query_id = '$plain_id'
      AND message LIKE '%Creating parallel replicas coordinator%'"

# The query must return the same rows as it does with parallel replicas out of the picture.
echo -n 'correlated subquery result matches non-parallel: '
with_pr=$(${CLICKHOUSE_CLIENT} --query "$correlated")
without_pr=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_cs_pr WHERE key IN (SELECT (SELECT key) FROM t_cs_pr_2 ORDER BY key LIMIT 10) SETTINGS enable_analyzer = 1, enable_parallel_replicas = 0, correlated_subqueries_use_in_memory_buffer = 0")
if [ -n "$with_pr" ] && [ "$with_pr" = "$without_pr" ]; then echo 1; else echo 0; fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cs_pr; DROP TABLE t_cs_pr_2"
