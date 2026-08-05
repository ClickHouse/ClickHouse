#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses several test clusters
# Random settings limits: automatic_parallel_replicas_mode=(0, 0)

# Each arm runs in its own client invocation and is followed by a liveness probe in a further
# invocation: the abort this test guards against happens on a follower connection, so the client that
# issued the query still receives the correct answer over its own session. A trailing query inside the
# same session is answered too. Only a new connection observes that the server is gone.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

PR_SETTINGS="allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3,
             parallel_replicas_for_non_replicated_merge_tree = 1"
# enable_analyzer = 1: the nested cluster() read only reaches a follower under the analyzer, so the
# arms below would pass on an unfixed binary in the old-analyzer CI jobs without it. Arm 6 cannot
# take it: its SETTINGS clause sits in a subquery, which validateAnalyzerSettings rejects.
ARM_SETTINGS="${PR_SETTINGS}, enable_analyzer = 1"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_pr_nested;
CREATE TABLE t_pr_nested (dt DateTime, idx Int32, i Nullable(UInt64))
ENGINE = MergeTree PARTITION BY dt ORDER BY idx;
INSERT INTO t_pr_nested SELECT toDateTime(number), number, number FROM numbers(100);
"

# $1 = label, $2 = query
arm() {
    echo "-- $1"
    ${CLICKHOUSE_CLIENT} -q "$2"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'server alive'"
}

arm "cluster() in an IN subquery, local_plan = 0" "
SELECT count() FROM t_pr_nested AS t
WHERE t.i IN (SELECT i FROM cluster('test_cluster_one_shard_two_replicas', currentDatabase(), 't_pr_nested'))
SETTINGS ${ARM_SETTINGS}, cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
         parallel_replicas_local_plan = 0"

arm "cluster() in an IN subquery, local_plan = 1" "
SELECT count() FROM t_pr_nested AS t
WHERE t.i IN (SELECT i FROM cluster('test_cluster_one_shard_two_replicas', currentDatabase(), 't_pr_nested'))
SETTINGS ${ARM_SETTINGS}, cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
         parallel_replicas_local_plan = 1"

arm "clusterAllReplicas() in an IN subquery" "
SELECT count() FROM t_pr_nested AS t
WHERE t.i IN (SELECT i FROM clusterAllReplicas('test_cluster_one_shard_two_replicas', currentDatabase(), 't_pr_nested'))
SETTINGS ${ARM_SETTINGS}, cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
         parallel_replicas_local_plan = 0"

arm "three replicas" "
SELECT count() FROM t_pr_nested AS t
WHERE t.i IN (SELECT i FROM cluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), 't_pr_nested'))
SETTINGS ${ARM_SETTINGS}, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_local_plan = 0"

arm "plan-based parallel replicas" "
SELECT count() FROM t_pr_nested AS t
WHERE t.i IN (SELECT i FROM cluster('test_cluster_one_shard_two_replicas', currentDatabase(), 't_pr_nested'))
SETTINGS ${ARM_SETTINGS}, cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
         parallel_replicas_local_plan = 0, parallel_replicas_plan_based = 1"

# An IN subquery over a local table must still be read with parallel replicas: the reset above must not
# stop a legitimate parallel-replicas read from being planned. This assertion does not depend on the
# analyzer, so instead of ARM_SETTINGS it takes parallel_replicas_only_with_analyzer = 0 to keep
# holding on the old-analyzer jobs.
echo "-- a local IN subquery is still read with parallel replicas"
${CLICKHOUSE_CLIENT} -q "
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_pr_nested AS t WHERE t.i IN (SELECT i FROM t_pr_nested)
    SETTINGS ${PR_SETTINGS}, cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
             parallel_replicas_local_plan = 0, parallel_replicas_only_with_analyzer = 0
) WHERE explain ILIKE '%ParallelReplicas%'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_pr_nested"
