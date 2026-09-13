#!/usr/bin/env bash
# `FETCH PARTITION` and `FETCH PART` copy base parts only, and a patch part - which holds an
# acknowledged lightweight update that its base parts do not have yet - lives in a partition of its
# own, so it was never fetched: the copy silently served pre-update values. Every other
# part-relocation command refuses that situation and points at `APPLY PATCHES`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

zk_prefix="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_fetch_patches_src SYNC;
DROP TABLE IF EXISTS t_fetch_patches_dst SYNC;

CREATE TABLE t_fetch_patches_src (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fetch_patches_src', 'r1')
PARTITION BY intDiv(id, 1000) ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_fetch_patches_dst (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fetch_patches_dst', 'r1')
PARTITION BY intDiv(id, 1000) ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_fetch_patches_src SELECT number, 0 FROM numbers(1000);
"

${CLICKHOUSE_CLIENT} --enable_lightweight_update 1 -q "UPDATE t_fetch_patches_src SET v = 42 WHERE id < 500"

${CLICKHOUSE_CLIENT} -q "
SELECT 'the update is acknowledged on the source', count(), sum(v) FROM t_fetch_patches_src;
SELECT 'and its patch part is still pending', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_fetch_patches_src' AND active AND startsWith(partition_id, 'patch');
"

echo -n 'the fetch of the partition is refused: '
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_fetch_patches_dst FETCH PARTITION 0 FROM '$zk_prefix/t_fetch_patches_src'" 2>&1 |
    grep -c -m1 'APPLY PATCHES'

part_name=$(${CLICKHOUSE_CLIENT} -q "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 't_fetch_patches_src' AND active AND partition_id = '0'
    ORDER BY name LIMIT 1")

echo -n 'and so is the fetch of a part: '
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_fetch_patches_dst FETCH PART '$part_name' FROM '$zk_prefix/t_fetch_patches_src'" 2>&1 |
    grep -c -m1 'APPLY PATCHES'

# Once the patches are applied, the base parts hold the update and the fetch produces a faithful copy.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_fetch_patches_src APPLY PATCHES IN PARTITION ID '0' SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} -q "
ALTER TABLE t_fetch_patches_dst FETCH PARTITION 0 FROM '$zk_prefix/t_fetch_patches_src';
ALTER TABLE t_fetch_patches_dst ATTACH PARTITION 0;
SELECT 'the copy has the update', count(), sum(v) FROM t_fetch_patches_dst;

DROP TABLE t_fetch_patches_dst SYNC;
DROP TABLE t_fetch_patches_src SYNC;
"
