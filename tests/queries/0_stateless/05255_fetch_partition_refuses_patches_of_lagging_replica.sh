#!/usr/bin/env bash
# A lightweight update commits its patch part on one replica; the others learn about it through a
# `GET_PART` log entry. Until a replica executes that entry, its `parts` list the patched base part
# but not the patch, and nothing stops `FETCH PARTITION` (the replica with the highest `log_pointer`)
# or `FETCH PART` (a random replica having the part) from choosing it as the source. The fetch must
# still be refused: the patch is looked up on every replica of the source, not only on the chosen one.
#
# Replica `r1` commits the update and is detached, so the only active replica is `r2`, which has
# fetches stopped and therefore no patch.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

zk_path="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fetch_lagging_src"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_fetch_lagging_src_r1 SYNC;
DROP TABLE IF EXISTS t_fetch_lagging_src_r2 SYNC;
DROP TABLE IF EXISTS t_fetch_lagging_dst SYNC;

CREATE TABLE t_fetch_lagging_src_r1 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('$zk_path', 'r1')
PARTITION BY intDiv(id, 1000) ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_fetch_lagging_src_r2 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('$zk_path', 'r2')
PARTITION BY intDiv(id, 1000) ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_fetch_lagging_dst (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fetch_lagging_dst', 'r1')
PARTITION BY intDiv(id, 1000) ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_fetch_lagging_src_r1;
SYSTEM STOP MERGES t_fetch_lagging_src_r2;

INSERT INTO t_fetch_lagging_src_r1 SELECT number, 0 FROM numbers(500);
SYSTEM SYNC REPLICA t_fetch_lagging_src_r2;

SYSTEM STOP FETCHES t_fetch_lagging_src_r2;
"

${CLICKHOUSE_CLIENT} --enable_lightweight_update 1 -q "UPDATE t_fetch_lagging_src_r1 SET v = 42 WHERE 1"

${CLICKHOUSE_CLIENT} -q "
SELECT 'r1 has the patch part', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_fetch_lagging_src_r1' AND active AND startsWith(partition_id, 'patch');
DETACH TABLE t_fetch_lagging_src_r1;
"

# Wait until `r1` is no longer active, so `r2` is the only replica a fetch can choose.
for _ in $(seq 1 600)
do
    active=$(${CLICKHOUSE_CLIENT} -q "
        SELECT active_replicas FROM system.replicas WHERE database = currentDatabase() AND table = 't_fetch_lagging_src_r2'")
    [ "$active" -eq 1 ] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "
SELECT 'the only active replica is r2', active_replicas, total_replicas FROM system.replicas
WHERE database = currentDatabase() AND table = 't_fetch_lagging_src_r2';
SELECT 'r2 has no patch part', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_fetch_lagging_src_r2' AND active AND startsWith(partition_id, 'patch');
"

echo -n 'the fetch of the partition is refused: '
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_fetch_lagging_dst FETCH PARTITION 0 FROM '$zk_path'" 2>&1 |
    grep -c -m1 'APPLY PATCHES'

base_part=$(${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 't_fetch_lagging_src_r2' AND active AND partition_id = '0'")

echo -n 'and so is the fetch of the patched part: '
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_fetch_lagging_dst FETCH PART '$base_part' FROM '$zk_path'" 2>&1 |
    grep -c -m1 'APPLY PATCHES'

${CLICKHOUSE_CLIENT} -q "
SELECT 'nothing was fetched', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fetch_lagging_dst';

ATTACH TABLE t_fetch_lagging_src_r1;
SYSTEM START FETCHES t_fetch_lagging_src_r2;
DROP TABLE t_fetch_lagging_dst SYNC;
DROP TABLE t_fetch_lagging_src_r1 SYNC;
DROP TABLE t_fetch_lagging_src_r2 SYNC;
"
