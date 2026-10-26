#!/usr/bin/env bash
# When a `merge()` table function's regex matches the table the outer query reads, the outer read
# and the child read derive the identical parallel replicas `stream_id`, and without the
# `enable_parallel_replicas` clear in `ReadFromMerge::createChildrenPlans` one follower builds
# several read pools under one `replica_num` and announces more than once into the same
# coordinator. Depending on follower interleaving the coordinator then threw either
# `Initiator received more initial requests than there are replicas` or
# `Duplicate announcement received for replica number N`.
#
# The table must live in the `default` database: a single-argument `merge()` resolves against the
# default database on each hop, so under a per-test database the follower fails with
# `CANNOT_EXTRACT_TABLE_STRUCTURE` before it ever announces. Because `default` is shared by every
# concurrently running test, the table name carries `$CLICKHOUSE_DATABASE` so that two runs of this
# test - the flaky check runs it many times at once - cannot drop or create each other's table. The
# regex is anchored to that unique name so no other table can leak into the merge.
# `index_granularity = 16` matters: at the default granularity the initiator's replica claims every
# mark range and the followers are cancelled before announcing, so the collision never fires. The
# ~1024 marks produced by 16384 rows are the verified floor for reddening on a pre-fix binary in
# both `parallel_replicas_local_plan` modes (at ~512 marks the local-plan mode stops reproducing),
# with slack kept small because the flaky check runs many copies of this test concurrently on a
# debug build, where the `parallel_replicas_local_plan = 0` coordination round-trips degrade
# sharply under load (heavier fixtures exceeded the 180s per-test limit).
#
# `allow_experimental_parallel_reading_from_replicas = 2` makes an unsupported-shape fallback to a
# plain local read an error instead of a silent success, and
# `ParallelReplicasHandleRequestMicroseconds > 0` on the initiator's `system.query_log` entry
# proves a follower survived past the announcement into the coordinator's request path (where the
# bug lives), like in `04545_parallel_replicas_projection_short_circuit_unknown_stream.sql` - so
# the test cannot silently stop exercising the announcement path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

table="t_merge_pr_dup_announce_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS default.${table} SYNC;

CREATE TABLE default.${table} (dt DateTime, idx Int32, i Nullable(UInt64))
ENGINE = MergeTree PARTITION BY (idx % 3) ORDER BY idx SETTINGS index_granularity = 16;

INSERT INTO default.${table} SELECT toDateTime(number), number, number FROM numbers(16384);
"

for local_plan in 0 1; do
    # $$ keeps the id unique across repeated runs sharing one database (query_log survives them).
    query_id="04836_${CLICKHOUSE_DATABASE}_$$_local_plan_${local_plan}"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "
    USE default;

    SET enable_analyzer = 1;
    SET allow_experimental_parallel_reading_from_replicas = 2;
    SET max_parallel_replicas = 3;
    SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
    SET parallel_replicas_for_non_replicated_merge_tree = 1;
    SET automatic_parallel_replicas_mode = 0;
    SET parallel_replicas_min_number_of_rows_per_replica = 0;
    SET parallel_replicas_local_plan = ${local_plan};
    -- The regression guarded here is in the query-based dispatch: each replica re-plans the outer
    -- query, so a follower can build several read pools under one replica_num and announce twice.
    -- The plan-based implementation does not distribute this shape at all (the GLOBAL IN subquery
    -- leaves a DelayedCreatingSetsStep, which insertParallelReplicasSplit refuses to distribute),
    -- so no follower reaches the coordinator and the assertion below would read 0.
    SET parallel_replicas_plan_based = 0;

    SELECT count() FROM default.${table}
    WHERE idx GLOBAL IN (i NOT IN (SELECT i FROM merge('^${table}\$') WHERE dt >= -2147483648));
    "

    # The count above would also be 1 if parallel replicas silently fell back to a plain local
    # read, and a coordinator can be created even when the initiator claims every mark range and
    # the followers are cancelled before they announce. Prove a follower made it past the
    # announcement into the coordinator's request path. The multi-statement query above logs a
    # QueryFinish row per statement under the same query_id, hence the query_kind filter.
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['ParallelReplicasHandleRequestMicroseconds'] > 0
    FROM system.query_log
    WHERE query_id = '$query_id'
      AND current_database = 'default' -- not currentDatabase(): the SELECT above ran after USE default
      AND type = 'QueryFinish'
      AND query_kind = 'Select'
      AND event_time >= now() - INTERVAL 600 SECOND"
done

$CLICKHOUSE_CLIENT --query "DROP TABLE default.${table} SYNC"
