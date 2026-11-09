#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database
# no-shared-merge-tree: FETCH PARTITION is not supported by SharedMergeTree
# no-replicated-database: the test creates replicas with explicit Keeper paths and replica names
# `clearUnusedPatchParts` removes a patch part as soon as the base parts of *its own* replica have been
# mutated past it by `APPLY PATCHES`, without waiting for the other replicas. A replica that has not
# executed the `GET_PART` of the patch then still holds the old base part, and no patch is left on any
# replica to reveal that the part is stale. `FETCH PARTITION` and `FETCH PART` must still refuse it:
# the materialized update is visible as a newer data version of the same rows on the other replica.
#
# Replica `r1` commits the update, applies the patches, removes the patch part and is detached, so the
# only active replica is `r2`, which has its replication queue stopped and still holds the old base part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

zk_path="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fetch_stale_src"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_fetch_stale_src_r1 SYNC;
DROP TABLE IF EXISTS t_fetch_stale_src_r2 SYNC;
DROP TABLE IF EXISTS t_fetch_stale_dst SYNC;

CREATE TABLE t_fetch_stale_src_r1 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('$zk_path', 'r1')
PARTITION BY intDiv(id, 1000) ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
    old_parts_lifetime = 1, cleanup_delay_period = 1, max_cleanup_delay_period = 1,
    cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0;

CREATE TABLE t_fetch_stale_src_r2 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('$zk_path', 'r2')
PARTITION BY intDiv(id, 1000) ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_fetch_stale_dst (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fetch_stale_dst', 'r1')
PARTITION BY intDiv(id, 1000) ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_fetch_stale_src_r1 SELECT number, 0 FROM numbers(500);
SYSTEM SYNC REPLICA t_fetch_stale_src_r2;

SYSTEM STOP REPLICATION QUEUES t_fetch_stale_src_r2;
"

${CLICKHOUSE_CLIENT} --enable_lightweight_update 1 -q "UPDATE t_fetch_stale_src_r1 SET v = 42 WHERE 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_fetch_stale_src_r1 APPLY PATCHES IN PARTITION ID '0' SETTINGS mutations_sync = 1"

# Wait until `r1` has removed the patch part, including from Keeper: a part leaves `system.parts`
# before it is removed from `/replicas/r1/parts`, and the fetch checks the latter.
for _ in $(seq 1 600)
do
    patches=$(${CLICKHOUSE_CLIENT} -q "
        SELECT
            (SELECT count() FROM system.parts
             WHERE database = currentDatabase() AND table = 't_fetch_stale_src_r1' AND startsWith(partition_id, 'patch'))
          + (SELECT count() FROM system.zookeeper
             WHERE path = '$zk_path/replicas/r1/parts' AND startsWith(name, 'patch'))")
    [ "$patches" -eq 0 ] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "
SELECT 'r1 has the update', count(), sum(v) FROM t_fetch_stale_src_r1;
SELECT 'r1 has no patch part left', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_fetch_stale_src_r1' AND startsWith(partition_id, 'patch');
DETACH TABLE t_fetch_stale_src_r1;
"

# Wait until `r1` is no longer active, so `r2` is the only replica a fetch can choose.
for _ in $(seq 1 600)
do
    active=$(${CLICKHOUSE_CLIENT} -q "
        SELECT active_replicas FROM system.replicas WHERE database = currentDatabase() AND table = 't_fetch_stale_src_r2'")
    [ "$active" -eq 1 ] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "
SELECT 'the only active replica is r2', active_replicas, total_replicas FROM system.replicas
WHERE database = currentDatabase() AND table = 't_fetch_stale_src_r2';
SELECT 'r2 has neither the update nor a patch part', count(), sum(v) FROM t_fetch_stale_src_r2;
"

echo -n 'the fetch of the partition is refused: '
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_fetch_stale_dst FETCH PARTITION 0 FROM '$zk_path'" 2>&1 |
    grep -c -m1 'SYSTEM SYNC REPLICA'

base_part=$(${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 't_fetch_stale_src_r2' AND active AND partition_id = '0'")

echo -n 'and so is the fetch of the stale part: '
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_fetch_stale_dst FETCH PART '$base_part' FROM '$zk_path'" 2>&1 |
    grep -c -m1 'SYSTEM SYNC REPLICA'

${CLICKHOUSE_CLIENT} -q "
SELECT 'nothing was fetched', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fetch_stale_dst';

ATTACH TABLE t_fetch_stale_src_r1;
SYSTEM START REPLICATION QUEUES t_fetch_stale_src_r2;
DROP TABLE t_fetch_stale_dst SYNC;
DROP TABLE t_fetch_stale_src_r1 SYNC;
DROP TABLE t_fetch_stale_src_r2 SYNC;
"
