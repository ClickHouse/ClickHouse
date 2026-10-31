#!/usr/bin/env bash
# Tags: zookeeper

# Patch parts carry data versions allocated from the block numbers of their own table. A copy in another table
# made updates there fail with "Found patch part ... that intersects mutation with version ...".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `remove_unused_patch_parts = 0` keeps the applied patches in `t_src` and any copies of them in `t_dst`.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_src (id UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_src', '1') ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, remove_unused_patch_parts = 0;

    CREATE TABLE t_dst AS t_src
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dst', '1') ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, remove_unused_patch_parts = 0;

    INSERT INTO t_src SELECT number, 0 FROM numbers(10);
    UPDATE t_src SET v = 1 WHERE id = 1;
    UPDATE t_src SET v = 2 WHERE id = 2;
    UPDATE t_src SET v = 3 WHERE id = 3;
"

patch_partition_id=$($CLICKHOUSE_CLIENT --query "
    SELECT DISTINCT partition_id FROM system.parts
    WHERE database = currentDatabase() AND table = 't_src' AND active AND startsWith(partition_id, 'patch-')")

# One patch part with data versions [1, 3], applied to the base part. `ATTACH PARTITION ALL` is what `CLONE AS` runs.
$CLICKHOUSE_CLIENT --query "
    OPTIMIZE TABLE t_src PARTITION ID '$patch_partition_id' FINAL;
    ALTER TABLE t_src APPLY PATCHES IN PARTITION ID 'all' SETTINGS mutations_sync = 2;
    ALTER TABLE t_dst ATTACH PARTITION ALL FROM t_src;

    SELECT 'patch parts in t_dst', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_dst' AND active AND startsWith(partition_id, 'patch-');

    UPDATE t_dst SET v = 10 WHERE id IN (1, 4);
    SELECT id, v FROM t_dst ORDER BY id;
"

# An unapplied patch, which a move of its partition would take away from `t_src`.
$CLICKHOUSE_CLIENT --query "UPDATE t_src SET v = 7 WHERE id = 7"

$CLICKHOUSE_CLIENT --query "ALTER TABLE t_dst ATTACH PARTITION ID '$patch_partition_id' FROM t_src" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_dst REPLACE PARTITION ID '$patch_partition_id' FROM t_src" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_src MOVE PARTITION ID '$patch_partition_id' TO TABLE t_dst" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

$CLICKHOUSE_CLIENT --query "SELECT 'v in t_src', v FROM t_src WHERE id = 7"
