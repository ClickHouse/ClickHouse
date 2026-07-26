#!/usr/bin/env bash
# Tags: shard

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# --query_kind=secondary_query makes the server treat the query as a shard-side one, which is the
# only state in which the marshalling gate in Planner::buildPlanForQueryNode() fires.
# --compression 1 is required as well: without a network codec the marshalling callback returns the
# block unchanged, so nothing is wrapped into ColumnBLOB.
CLIENT="$CLICKHOUSE_CLIENT --compression 1 --query_kind=secondary_query"

MARSHALLING_ON="enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 1"
MARSHALLING_OFF="enable_parallel_blocks_marshalling = 0, prefer_localhost_replica = 1"
PARALLEL_REPLICAS="automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1, parallel_replicas_local_plan = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, prefer_localhost_replica = 1"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE tab (x UInt64, y UInt64, z UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO tab SELECT number % 7, number, number FROM numbers(1000);
"

# GROUPING SETS with 2+ sets, so MergingAggregatedTransform::addChunk() type-checks __grouping_set
# instead of returning early. A ColumnBLOB reaching it used to throw
# "Expected UInt64 column for __grouping_set, got BLOB".
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

# A single grouping set takes the early-return branch of addChunk() and never type-checked the
# column, so it worked even with a ColumnBLOB. Pin it so the branch the fix does not target
# cannot regress either.
echo '-- single grouping set'
$CLIENT -q "
    SELECT count(), sum(cityHash64(x, s)) FROM (
        SELECT x, sum(y) AS s FROM remote('127.0.0.{1,2}', currentDatabase(), tab)
        GROUP BY GROUPING SETS ((x))
    ) SETTINGS $MARSHALLING_ON"

# An ordinary (non-secondary) query never marshals, so the fix must not change it.
echo '-- initial query'
$CLICKHOUSE_CLIENT --compression 1 -q "$GROUPING_SETS_QUERY SETTINGS $MARSHALLING_ON"

# The plan shape is the invariant being fixed: the locally executed branch must carry no
# BlocksMarshalling step, while the branch read from a remote replica must keep it.
# Asserted as counts, not as a plan diff, so unrelated plan formatting changes do not break it.
echo '-- BlocksMarshalling steps: total, and how many appear before ReadFromRemote'
$CLIENT -q "
    SELECT
        countIf(explain ILIKE '%BlocksMarshalling%'),
        countIf(explain ILIKE '%BlocksMarshalling%' AND rn < remote_rn)
    FROM (
        SELECT explain, rowNumberInAllBlocks() AS rn, min(if(explain ILIKE '%ReadFromRemote%', rn, NULL)) OVER () AS remote_rn
        FROM (
            EXPLAIN distributed = 1
            SELECT x, sum(y) FROM remote('127.0.0.{1,2}', currentDatabase(), tab)
            GROUP BY GROUPING SETS ((x, z % 1), (intDiv(z, 100), z + 2))
            SETTINGS $MARSHALLING_ON
        )
    )
    SETTINGS serialize_query_plan = 0, explain_query_plan_default = 'legacy'"
