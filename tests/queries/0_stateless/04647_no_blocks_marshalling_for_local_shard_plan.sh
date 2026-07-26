#!/usr/bin/env bash
# Tags: shard

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `--query_kind=secondary_query` makes the server treat the query as a shard-side one, which is the
# only state in which the marshalling gate in `Planner::buildPlanForQueryNode` fires.
# `--compression 1` is required as well: without a network codec the marshalling callback returns
# the block unchanged, so nothing is wrapped into `ColumnBLOB`.
# `enable_analyzer` is pinned because `BlocksMarshallingStep` is only ever added by the analyzer,
# and CI runs variants with `allow_experimental_analyzer = 0`.
# `serialize_query_plan` is pinned to 0 for the queries that are executed: a plan serialized and
# shipped to a shard legitimately still carries `BlocksMarshalling`, which is not registered in
# `QueryPlanStepRegistry` on master, so executing one fails with `Unknown query plan step:
# BlocksMarshalling` independently of this fix. The `distributed plan` CI variants set
# `serialize_query_plan = 1` in the default profile.
CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer 1 --compression 1 --serialize_query_plan 0 --query_kind=secondary_query"
INITIAL_CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer 1 --compression 1 --serialize_query_plan 0"

MARSHALLING_ON="enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 1"
MARSHALLING_OFF="enable_parallel_blocks_marshalling = 0, prefer_localhost_replica = 1"
# Mode 2 throws instead of silently falling back to plain local execution, so an unavailable
# cluster fails the test instead of making the parallel replicas cases assert nothing.
PARALLEL_REPLICAS="automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 2, parallel_replicas_local_plan = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, prefer_localhost_replica = 1"

$INITIAL_CLIENT -q "
    CREATE TABLE tab (x UInt64, y UInt64, z UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO tab SELECT number % 7, number, number FROM numbers(1000);
"

# Counts of `BlocksMarshalling` steps in a distributed plan: the total, how many appear above the
# step named in $2 that reads from another replica, and how many such reading steps there are.
# Asserted as counts, not as a plan diff, so unrelated plan formatting changes do not break it.
marshalling_counts() {
    $CLIENT -q "
        SELECT
            countIf(explain ILIKE '%BlocksMarshalling%'),
            countIf(explain ILIKE '%BlocksMarshalling%' AND rn < remote_rn),
            countIf(explain ILIKE '%$2%')
        FROM (
            SELECT
                explain,
                rowNumberInAllBlocks() AS rn,
                min(if(explain ILIKE '%$2%', rn, NULL)) OVER () AS remote_rn
            FROM ( EXPLAIN distributed = 1 $1 )
        )
        SETTINGS explain_query_plan_default = 'legacy'"
}

# `GROUPING SETS` with 2+ sets, so `MergingAggregatedTransform::addChunk` type-checks
# `__grouping_set` instead of returning early. A `ColumnBLOB` reaching it used to throw
# `Expected UInt64 column for __grouping_set, got BLOB`.
GROUPING_SETS_QUERY="
    SELECT count(), sum(cityHash64(x, s)) FROM (
        SELECT x, sum(y) AS s FROM remote('127.0.0.{1,2}', currentDatabase(), tab)
        GROUP BY GROUPING SETS ((x, z % 1), (intDiv(z, 100), z + 2))
    )"

echo '-- local shard plan, several grouping sets: with and without marshalling'
$CLIENT -q "$GROUPING_SETS_QUERY SETTINGS $MARSHALLING_ON"
$CLIENT -q "$GROUPING_SETS_QUERY SETTINGS $MARSHALLING_OFF"

PARALLEL_REPLICAS_QUERY="
    SELECT count(), sum(cityHash64(x, s)) FROM (
        SELECT x, sum(y) AS s FROM tab
        GROUP BY GROUPING SETS ((x, z % 1), (intDiv(z, 100), z + 2))
    )"

echo '-- parallel replicas local plan, several grouping sets: with and without marshalling'
$CLIENT -q "$PARALLEL_REPLICAS_QUERY SETTINGS enable_parallel_blocks_marshalling = 1, $PARALLEL_REPLICAS"
$CLIENT -q "$PARALLEL_REPLICAS_QUERY SETTINGS enable_parallel_blocks_marshalling = 0, $PARALLEL_REPLICAS"

# A single grouping set takes the early-return branch of `addChunk` and never type-checked the
# column, so it worked even with a `ColumnBLOB`. Pin it so the branch the fix does not target
# cannot regress either.
echo '-- single grouping set'
$CLIENT -q "
    SELECT count(), sum(cityHash64(x, s)) FROM (
        SELECT x, sum(y) AS s FROM remote('127.0.0.{1,2}', currentDatabase(), tab)
        GROUP BY GROUPING SETS ((x))
    ) SETTINGS $MARSHALLING_ON"

# CONTROL: an ordinary (non-secondary) query never marshals and passes without the fix too, so the
# fix must not change it. Do not mistake it for one of the assertions above.
echo '-- initial query (control)'
$INITIAL_CLIENT -q "$GROUPING_SETS_QUERY SETTINGS $MARSHALLING_ON"

# A `Merge` table builds a child plan per underlying table with its own `SelectQueryOptions`, and
# unites them in this process with no unmarshalling. With a marshalling producer in the set the
# stage of the `MergeTree` child degrades, so it is planned by the analyzer and used to get its own
# marshalling step, which then reached `ExpressionActions::executeOnColumns` as a `ColumnBLOB`.
# Uses $INITIAL_CLIENT on purpose: the secondary context comes from the inner `remote(...)` hop.
echo '-- Merge table over a local and a distributed table'
$INITIAL_CLIENT -q "
    CREATE TABLE mrg_local (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO mrg_local SELECT number FROM numbers(1000);
    CREATE TABLE mrg_dist (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), mrg_local);
    CREATE TABLE mrg_all (x UInt64) ENGINE = Merge(currentDatabase(), '^mrg_(local|dist)\$');
"
$INITIAL_CLIENT -q "
    SELECT count(), sum(cityHash64(x)) FROM remote('127.0.0.1', currentDatabase(), mrg_all)
    SETTINGS enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 0"
$INITIAL_CLIENT -q "
    SELECT count(), sum(cityHash64(x)) FROM remote('127.0.0.1', currentDatabase(), mrg_all)
    SETTINGS enable_parallel_blocks_marshalling = 0, prefer_localhost_replica = 0"

# The plan shape is the invariant being fixed: the locally executed branch must carry no
# `BlocksMarshalling` step, while the branch read from a remote replica must keep it.
echo '-- BlocksMarshalling steps for remote(): total, above ReadFromRemote, ReadFromRemote steps'
marshalling_counts "
    SELECT x, sum(y) FROM remote('127.0.0.{1,2}', currentDatabase(), tab)
    GROUP BY GROUPING SETS ((x, z % 1), (intDiv(z, 100), z + 2))
    SETTINGS $MARSHALLING_ON, serialize_query_plan = 0" "ReadFromRemote"

echo '-- BlocksMarshalling steps for parallel replicas: total, above the remote step, remote steps'
marshalling_counts "
    SELECT x, sum(y) FROM tab
    GROUP BY GROUPING SETS ((x, z % 1), (intDiv(z, 100), z + 2))
    SETTINGS enable_parallel_blocks_marshalling = 1, $PARALLEL_REPLICAS, serialize_query_plan = 0" "ReadFromRemoteParallelReplicas"

# A plan that is serialized and shipped to a shard must keep marshalling: the shard sends its blocks
# back over the network. EXPLAIN only, executing such a plan fails with
# `Unknown query plan step: BlocksMarshalling` both with and without this fix.
echo '-- BlocksMarshalling steps for a serialized plan: total, above ReadFromRemote, ReadFromRemote steps'
marshalling_counts "
    SELECT x, sum(y) FROM remote('127.0.0.{1,2}', currentDatabase(), tab)
    GROUP BY GROUPING SETS ((x, z % 1), (intDiv(z, 100), z + 2))
    SETTINGS $MARSHALLING_ON, serialize_query_plan = 1" "ReadFromRemote"
