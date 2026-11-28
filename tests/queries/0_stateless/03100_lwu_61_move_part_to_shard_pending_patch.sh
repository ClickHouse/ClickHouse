#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database
# Tag no-shared-merge-tree: SharedMergeTree does not move parts between shards.
# Tag no-replicated-database: the tables use their own Keeper paths.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DST="/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst"
COLUMNS="part_key Int64, id Int64, val Int64, tag String"
SETTINGS="PARTITION BY part_key ORDER BY id SETTINGS assign_part_uuids = 1, part_moves_between_shards_enable = 1,
    part_moves_between_shards_delay_seconds = 0, enable_block_number_column = 1, enable_block_offset_column = 1"
CLIENT="$CLICKHOUSE_CLIENT --enable_lightweight_update=1 --apply_patch_parts=1 --insert_keeper_fault_injection_probability=0"
PATCHES="SELECT countIf(table = 'src_r1') > 0, countIf(table = 'src_r2') > 0 FROM system.parts
    WHERE database = currentDatabase() AND active AND name LIKE 'patch-%'"
PART="SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'src_r1' AND active AND partition_id = '1'"

function move_part()
{
    $CLIENT -q "ALTER TABLE src_r1 MOVE PART '$($CLIENT -q "$PART")' TO SHARD '$DST'" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
}

$CLIENT -q "
CREATE TABLE src_r1 ($COLUMNS) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', 'r1') $SETTINGS;
CREATE TABLE src_r2 ($COLUMNS) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', 'r2') $SETTINGS;
CREATE TABLE dst ($COLUMNS) ENGINE = ReplicatedMergeTree('$DST', 'r1') $SETTINGS;
INSERT INTO src_r1 VALUES (1, 1, 100, 'original'), (1, 2, 200, 'deleted');
SYSTEM SYNC REPLICA src_r2;
SYSTEM STOP FETCHES src_r1;
UPDATE src_r2 SET val = 999, tag = 'updated' WHERE id = 1;
"

# The patch part is only on the other replica, which this one has not fetched yet.
$CLIENT -q "$PATCHES"
move_part

$CLIENT -q "
SYSTEM START FETCHES src_r1;
SYSTEM SYNC REPLICA src_r1;
DELETE FROM src_r1 WHERE id = 2 SETTINGS lightweight_delete_mode = 'lightweight_update_force';
"

# This replica has patch parts of its own.
$CLIENT -q "$PATCHES"
move_part
$CLIENT -q "
SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table = 'src_r1';
SELECT id, val, tag FROM src_r1 ORDER BY id;
"

# After APPLY PATCHES the part itself carries the changes, so it can be moved.
$CLIENT --mutations_sync=2 -q "ALTER TABLE src_r1 APPLY PATCHES IN PARTITION ID '1'"
$CLIENT -q "ALTER TABLE src_r1 MOVE PART '$($CLIENT -q "$PART")' TO SHARD '$DST'"
$CLIENT -q "
SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table = 'src_r1';
DROP TABLE src_r1 SYNC;
DROP TABLE src_r2 SYNC;
DROP TABLE dst SYNC;
"
