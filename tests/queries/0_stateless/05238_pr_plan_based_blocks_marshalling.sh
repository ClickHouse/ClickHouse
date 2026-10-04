#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `parallel_replicas_plan_based`, a replica executes a plan fragment shipped by the initiator and
# never runs the planner, so the marshalling gate in `Planner::buildPlanForQueryNode` cannot fire there.
# The fragment must therefore be given a `BlocksMarshalling` step by the initiator, exactly like the
# query-tree-based implementation gets one on each replica (03403, 04647).
#
# `--compression 1` is required: without a network codec the marshalling callback returns the block
# unchanged, so nothing is wrapped into `ColumnBLOB`.
CLIENT="$CLICKHOUSE_CLIENT --compression 1 --enable_analyzer 1"

# Mode 2 throws instead of silently falling back to plain local execution, so an unavailable cluster
# fails the test instead of making it assert nothing.
PARALLEL_REPLICAS="parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, prefer_localhost_replica = 1"

$CLIENT -q "
    DROP TABLE IF EXISTS tab;
    CREATE TABLE tab (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO tab SELECT number % 7, number FROM numbers(1000);
"

# Counts of `BlocksMarshalling` steps in a distributed plan: the total, how many appear above the
# `ReadFromParallelReplicas` step, and how many such reading steps there are. Asserted as counts, not
# as a plan diff, so unrelated plan formatting changes do not break it. Only prints the query, so
# that several of these can share one client invocation.
#
# The step names are matched at the start of the line: the description of `ReadFromParallelReplicas`
# is a dump of the shipped fragment, so a substring match would count the steps of that dump too.
marshalling_counts_query() {
    echo "
        SELECT
            countIf(is_marshalling),
            countIf(is_marshalling AND rn < remote_rn),
            countIf(is_remote)
        FROM (
            SELECT
                match(explain, '^ *BlocksMarshalling') AS is_marshalling,
                match(explain, '^ *ReadFromParallelReplicas') AS is_remote,
                rowNumberInAllBlocks() AS rn,
                min(if(is_remote, rn, NULL)) OVER () AS remote_rn
            FROM ( EXPLAIN distributed = 1 $1 )
        )
        SETTINGS explain_query_plan_default = 'legacy'"
}

AGGREGATING_QUERY="SELECT x, sum(y) FROM tab GROUP BY x ORDER BY x"
# No `LIMIT`: with a full top-N sort above it (which is what the plan looks like once the randomized
# `optimize_read_in_order = 0` of the flaky check removes the read-in-order plan), `use_top_k_dynamic_filtering`
# selects the read for the Top-K filter optimization, and `mergeTreeReadCanBeShipped` then keeps that read
# local - leaving no remote fragment to assert about.
PLAIN_QUERY="SELECT x, y FROM tab WHERE y % 250 = 0 ORDER BY x, y"

# Exactly one `BlocksMarshalling` (the fragment is serialized once and reused for every replica), and
# it must be under the remote step: the branch executed in this process is united into the parent
# pipeline, where nothing unmarshalls the blocks.
$CLIENT -q "
    SELECT '-- total, above ReadFromParallelReplicas, ReadFromParallelReplicas steps';
    $(marshalling_counts_query "$AGGREGATING_QUERY SETTINGS $PARALLEL_REPLICAS, parallel_replicas_local_plan = 1, enable_parallel_blocks_marshalling = 1");
    $(marshalling_counts_query "$AGGREGATING_QUERY SETTINGS $PARALLEL_REPLICAS, parallel_replicas_local_plan = 1, enable_parallel_blocks_marshalling = 0");
    $(marshalling_counts_query "$PLAIN_QUERY SETTINGS $PARALLEL_REPLICAS, parallel_replicas_local_plan = 1, enable_parallel_blocks_marshalling = 1");
    $(marshalling_counts_query "$AGGREGATING_QUERY SETTINGS $PARALLEL_REPLICAS, parallel_replicas_local_plan = 0, enable_parallel_blocks_marshalling = 1");
"

# Executing the queries is what exercises the replica side: the shipped fragment now carries a step
# that has to be in `QueryPlanStepRegistry` to deserialize, and a fragment ending in a partial
# aggregation still has to be recognized as such under the marshalling step.
$CLIENT -q "
    SELECT '-- aggregating, local plan, marshalling on';
    $AGGREGATING_QUERY SETTINGS $PARALLEL_REPLICAS, parallel_replicas_local_plan = 1, enable_parallel_blocks_marshalling = 1;

    SELECT '-- aggregating, local plan, marshalling off';
    $AGGREGATING_QUERY SETTINGS $PARALLEL_REPLICAS, parallel_replicas_local_plan = 1, enable_parallel_blocks_marshalling = 0;

    SELECT '-- plain, local plan, marshalling on';
    $PLAIN_QUERY SETTINGS $PARALLEL_REPLICAS, parallel_replicas_local_plan = 1, enable_parallel_blocks_marshalling = 1;

    SELECT '-- aggregating, no local plan, marshalling on';
    $AGGREGATING_QUERY SETTINGS $PARALLEL_REPLICAS, parallel_replicas_local_plan = 0, enable_parallel_blocks_marshalling = 1;
"
